import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
import { completeJob, failJob, startJob } from '../utils/cronTracker.js';

dotenv.config();

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

const cloudFlareUrl = 'https://codes-findlaw-scraper.tejasw.workers.dev/api';

function sleep(time) {
    return new Promise((resolve) => setTimeout(resolve, time));
}

const CONFIG = {
    maxSitemaps: 1, // Maximum number of sitemaps to process (reduced for testing)
    maxEntriesPerSitemap: 4, // Maximum number of entries to process per sitemap (reduced for testing)
    filterBatchSize: 10000, // Size of batches for filtering URLs (larger)
    cloudFlareBatchSize: 100, // Size of batches for Cloudflare requests (smaller)
    requestInterval: 0, // Delay in milliseconds between batches - Adjust as needed
};

async function fetchWithRetry(url, retries = 5, delayMs = 2000) {
    for (let i = 0; i < retries; i++) {
        const response = await fetch(url);
        if (response.ok) {
            return response;
        } else if (i < retries - 1) {
            // console.warn(`${response.status} - Retrying ${i + 1}/${retries}`);
            await sleep(delayMs);
        }
    }
    return {
        url,
        status: 'error',
        error: 'Failed after multiple retries',
    };
}

/**
 * Insert case law data into the caselaw_us table if it doesn't already exist
 * @param caseData - The case data to insert
 * @param sourceUrl - The URL from which the case was scraped
 * @returns The ID of the case record
 */
async function insertCaseLaw(caseData, sourceUrl) {
    try {
        const { data, error } = await supabase.from('caselaw_us').insert([caseData]).select('id').single();

        // Record that this URL has been processed
        const { error: urlInsertError } = await supabase.from('caselaw_scraping_urls').upsert(
            [
                {
                    url: sourceUrl,
                    case_id: data.id,
                    processed: true,
                    processing_date: new Date(),
                    status: 'success',
                },
            ],
            { onConflict: 'url' }
        );

        if (error) console.log('error', error);
        if (data) console.log('data', data.id);

        if (urlInsertError) throw urlInsertError;

        return data.id;
    } catch (error) {
        // Record the error for this URL
        await supabase.from('caselaw_scraping_urls').upsert(
            [
                {
                    url: sourceUrl,
                    processed: false,
                    processing_date: new Date(),
                    status: 'error',
                    error_message: error.message,
                },
            ],
            { onConflict: 'url' }
        );

        console.error('Error inserting case law:', error);
        throw error;
    }
}

// Add this function to check if a URL has been processed
async function checkIfUrlProcessed(url) {
    const { data, error } = await supabase
        .from('caselaw_scraping_urls')
        .select('processed')
        .eq('url', url)
        .eq('processed', true)
        .maybeSingle();

    if (error && error.code !== 'PGRST116') {
        console.error('Error checking URL status:', error);
        return false;
    }

    return !!data;
}

async function scrapFindLaws() {
    const JOB_NAME = 'us_caselaw_scraper';
    let runId = null;
    let newCasesFound = 0;
    let sitemapsProcessed = 0;

    try {
        // Start job tracking
        const jobStart = await startJob(supabase, JOB_NAME);
        runId = jobStart.runId;
        if (!jobStart.success) {
            console.warn('Warning: Failed to start job tracking. Continuing without tracking.');
        }

        let siteMapResponse = await fetch(`${cloudFlareUrl}/sitemaps/cases`);
        let siteMapUrls = await siteMapResponse.json(); // Convert response to JSON

        // Limit sitemaps to process based on config
        const sitemapsToProcess = siteMapUrls.slice(0, CONFIG.maxSitemaps);

        for (let siteMapUrl of sitemapsToProcess) {
            let pageUrlRes = await fetch(`${cloudFlareUrl}/pages?sitemap=${siteMapUrl}`);
            let pageUrls = await pageUrlRes.json();

            console.log('pageUrls', pageUrls.length, siteMapUrl, CONFIG.filterBatchSize);
            sitemapsProcessed++;

            // Process URLs in larger batches to filter already processed ones
            for (let i = 0; i < pageUrls.length; i += CONFIG.filterBatchSize) {
                const filterBatch = pageUrls.slice(i, i + CONFIG.filterBatchSize);

                // Check which URLs have already been processed
                const checkPromises = filterBatch.map(async (url) => {
                    const isProcessed = await checkIfUrlProcessed(url);
                    return { url, isProcessed };
                });

                const checkedUrls = await Promise.all(checkPromises);
                const urlsToProcess = checkedUrls.filter((item) => !item.isProcessed).map((item) => item.url);

                console.log(`Filtered batch: ${filterBatch.length} URLs checked, ${urlsToProcess.length} need processing`);

                // Process the filtered URLs in smaller batches for Cloudflare requests
                for (let j = 0; j < urlsToProcess.length; j += CONFIG.cloudFlareBatchSize) {
                    const cloudFlareBatch = urlsToProcess.slice(j, j + CONFIG.cloudFlareBatchSize);

                    const fetchPromises = cloudFlareBatch.map(async (url) => {
                        try {
                            const fullUrl = `${cloudFlareUrl}/scrape/cases?url=${url}`;
                            const response = await fetchWithRetry(fullUrl);

                            // Check if response is the error object returned by fetchWithRetry
                            if ('ok' in response && response.ok) {
                                const data = await response.json();
                                return { ...data, url };
                            } else {
                                return {
                                    url,
                                    status: 'error',
                                    error: 'ok' in response ? `HTTP error! Status: ${response.status}` : response.error,
                                };
                            }
                        } catch (err) {
                            return { url, status: 'error', error: err.message };
                        }
                    });

                    const results = await Promise.all(fetchPromises);

                    //Storing in DB
                    for (const result of results) {
                        if (result.status === 'error') {
                            console.log(result);
                            continue;
                        }
                        try {
                            //insert case data with source URL
                            await insertCaseLaw(
                                {
                                    court_name: result.court_name,
                                    case_name: result.case_name,
                                    case_no: result.case_no,
                                    date: result.date,
                                    case_text: result.case_text,
                                    citation: result.citation,
                                    country: 'US',
                                },
                                result.url
                            );

                            newCasesFound++;
                            console.log('result', result);
                        } catch (error) {
                            result.status = 'error';
                            result.error = error.message;
                            console.log('resulterror', result);
                        }
                    }

                    // Add a small delay between Cloudflare batches
                    await sleep(500);
                }
            }

            await sleep(1000);
        }

        console.log(`US case law scraping completed. Found ${newCasesFound} new cases. Processed ${sitemapsProcessed} sitemaps.`);

        // Record job completion
        await completeJob(supabase, runId, {
            new_cases_found: newCasesFound,
            pages_processed: sitemapsProcessed
        });

    } catch (error) {
        console.error(`Fatal error in US case law scraping: ${error.message}`);

        // Record job failure
        await failJob(supabase, runId, {
            new_cases_found: newCasesFound,
            pages_processed: sitemapsProcessed
        }, error.message);

        throw error;
    }
}

// Execute the main function
scrapFindLaws().then(() => console.log('done')); 