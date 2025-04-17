import { createClient } from '@supabase/supabase-js';

import dotenv from 'dotenv';

dotenv.config();

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY)

// URL for the Singapore Cloudflare worker
const cloudFlareUrl = 'https://votum-scraper-singapore.tejasw.workers.dev/api';

function sleep(time) {
    return new Promise(resolve => setTimeout(resolve, time));
}

const CONFIG = {
    maxPages: 100000000,           // Safety limit on maximum number of pages to process
    maxEntriesPerPage: 10,     // Maximum number of entries to process per page
    filterBatchSize: 10000,    // Size of batches for filtering URLs
    requestInterval: 1000,      // Delay in milliseconds between pages
};

async function fetchWithRetry(url, retries = 5, delayMs = 2000) {
    for (let i = 0; i < retries; i++) {
        const response = await fetch(url);
        if (response.ok) {
            return response;
        } else if (i < retries - 1) {
            await sleep(delayMs);
        }
    }
    return {
        url, status: "error", error: "Failed after multiple retries"
    }
}

/**
 * Check if a case with the given citation already exists in the database
 * @param citation - The citation to check
 * @returns True if the citation exists, false otherwise
 */
async function checkIfCitationExists(citation) {
    if (!citation) return false;

    const { data, error } = await supabase
        .from('caselaw_singapore')
        .select('id')
        .eq('citation', citation)
        .maybeSingle();

    if (error && error.code !== 'PGRST116') {
        console.error('Error checking citation status:', error);
        return false;
    }

    return !!data;
}

/**
 * Insert Singapore case law data into the caselaw_singapore table
 * @param caseData - The case data to insert
 * @param sourceUrl - The URL from which the case was scraped
 * @returns The ID of the case record
 */
async function insertCaseLaw(caseData, sourceUrl) {

    try {
        // Check if the citation already exists
        if (caseData.citation) {
            const citationExists = await checkIfCitationExists(caseData.citation);
            if (citationExists) {
                console.log(`Skipping case with existing citation: ${caseData.citation}`);
                return null;
            }
        }

        if (caseData.case_text.trim().length == 0) {
            console.log(`Skipping case with empty text: ${caseData.case_name}`);
            return null;
        }

        const { data, error } = await supabase
            .from('caselaw_singapore')
            .insert([{
                court_name: caseData.court_name,
                case_name: caseData.case_name,
                case_no: caseData.case_no,
                date: caseData.date,
                case_text: caseData.case_text,
                citation: caseData.citation,
                country: 'Singapore'
            }])
            .select('id')
            .single();


        if (data) {
            console.log(data);
        }
        if (error) {
            console.log(error);
        }

        if (error) throw error;

        // Record that this URL has been processed
        const { error: urlInsertError } = await supabase
            .from('caselaw_scraping_urls')
            .upsert([{
                url: sourceUrl,
                case_id: data.id,
                processed: true,
                processing_date: new Date(),
                status: 'success',
                country: 'Singapore'
            }], { onConflict: 'url' });

        if (urlInsertError) throw urlInsertError;

        console.log(`Inserted case: ${caseData.case_name} (ID: ${data.id})`);
        return data.id;
    } catch (error) {
        // Record the error for this URL
        await supabase
            .from('caselaw_scraping_urls')
            .upsert([{
                url: sourceUrl,
                processed: false,
                processing_date: new Date(),
                status: 'error',
                error_message: error.message,
                country: 'Singapore'
            }], { onConflict: 'url' });

        console.error('Error inserting Singapore case law:', error);
        throw error;
    }
}

// Check if a URL has been processed
async function checkIfUrlProcessed(url) {
    return false;
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

async function scrapeSingaporeCaseLaws() {
    try {
        let pageIndex = 340;
        let hasMorePages = true;

        // Process pages until no more results or safety limit reached
        while (hasMorePages && pageIndex <= CONFIG.maxPages) {
            console.log(`Processing page ${pageIndex}...`);

            // Get list of case URLs for current page
            const url = `${cloudFlareUrl}/sitemap/cases?index=${pageIndex}`;
            console.log(url);
            const casesResponse = await fetch(url);
            if (!casesResponse.ok) {
                console.error(`Failed to fetch cases for page ${pageIndex}: ${casesResponse}`);
                hasMorePages = false;
                continue;
            }

            const caseUrls = await casesResponse.json();
            if (!caseUrls || !caseUrls.length) {
                console.log(`No cases found on page ${pageIndex}, stopping pagination`);
                hasMorePages = false;
                break;
            }

            console.log(`Found ${caseUrls.length} cases on page ${pageIndex}`);

            // Limit the number of cases processed per page if needed
            const casesToProcess = caseUrls.slice(0, CONFIG.maxEntriesPerPage);

            // Check which URLs have already been processed
            const checkPromises = casesToProcess.map(async (url) => {
                const isProcessed = await checkIfUrlProcessed(url);
                return { url, isProcessed };
            });

            const checkedUrls = await Promise.all(checkPromises);
            const urlsToProcess = checkedUrls.filter(item => !item.isProcessed).map(item => item.url);

            console.log(`${urlsToProcess.length} cases need processing on page ${pageIndex}`);

            // Process all cases from the current page at once
            const fetchPromises = urlsToProcess.map(async (url) => {
                try {
                    // First try without isOld parameter
                    let fullUrl = `${cloudFlareUrl}/scrape/cases?url=${encodeURIComponent(`https://www.elitigation.sg${url}`)}`;
                    console.log(`First attempt: ${fullUrl}`);
                    let response = await fetchWithRetry(fullUrl);

                    if ('ok' in response && response.ok) {
                        let data = await response.json();

                        // If case_text is blank, try again with isOld=true
                        if (!data.case_text || data.case_text.trim().length === 0) {
                            console.log(`Blank case_text found, retrying with isOld=true for: ${url}`);
                            fullUrl = `${cloudFlareUrl}/scrape/cases?url=${encodeURIComponent(`https://www.elitigation.sg${url}`)}&isOld=true`;
                            console.log(`Second attempt: ${fullUrl}`);
                            response = await fetchWithRetry(fullUrl);

                            if ('ok' in response && response.ok) {
                                data = await response.json();
                            }
                        }

                        return { ...data, url };
                    } else {
                        return {
                            url,
                            status: 'error',
                            error: 'ok' in response ? `HTTP error! Status: ${response.status}` : response.error
                        };
                    }
                } catch (err) {
                    return { url, status: "error", error: err.message };
                }
            });

            const results = await Promise.all(fetchPromises);

            // Store results in database
            for (const result of results) {
                if (result.status === 'error') {
                    console.error(`Error scraping ${result.url}: ${result.error}`);
                    continue;
                }

                try {
                    await insertCaseLaw(result, result.url);
                } catch (error) {
                    console.error(`Failed to insert case from ${result.url}: ${error.message}`);
                }
            }

            // Add a delay between pages
            if (pageIndex < CONFIG.maxPages) {
                await sleep(CONFIG.requestInterval);
            }

            pageIndex++;
        }

        console.log('Singapore case law scraping completed');
    } catch (error) {
        console.error('Fatal error in Singapore case law scraping:', error);
    }
}

// Execute the scraping function
scrapeSingaporeCaseLaws().then(() => console.log('Singapore case law scraping process finished')); 
