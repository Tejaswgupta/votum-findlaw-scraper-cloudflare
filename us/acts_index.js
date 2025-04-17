import { createClient } from '@supabase/supabase-js';

const supabase = createClient('https://supabase.thevotum.com', "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.ewogICJyb2xlIjogImFub24iLAogICJpc3MiOiAic3VwYWJhc2UiLAogICJpYXQiOiAxNzE3OTU3ODAwLAogICJleHAiOiAxODc1NzI0MjAwCn0.XrCbkNQDLY0fvtqJ7ZHdimDSihI7sRfbqtIjqOXgrNg")

const cloudFlareUrl = 'https://codes-findlaw-scraper.tejasw.workers.dev/api';

async function getExistingActId(actName) {
        try {
                const { data, error } = await supabase
                        .from('acts')
                        .select('act_id')
                        .eq('act_name', actName)
                        .single();
                if (error) {
                        if (error.code === 'PGRST116') {  // No rows returned
                                return null;
                        }
                        throw error;
                }
                return data?.act_id;
        } catch (error) {
                console.error('Error checking existing act:', error);
                throw error;
        }
}

// Function to insert act and get its ID
async function insertAct(actName) {
        try {
                // First check if the act already exists
                const existingActId = await getExistingActId(actName);
                if (existingActId) {
                        return existingActId;
                }

                // If act doesn't exist, insert it
                const { data, error } = await supabase
                        .from('acts')
                        .insert([
                                { act_name: actName, country: 'US' }
                        ])
                        .select()
                        .single();

                if (error) throw error;
                return data.act_id;
        } catch (error) {
                // console.error('Error inserting act:', error);
                throw error;
        }
}

// Function to insert section
async function insertSection(actId, sectionData) {
        try {
                // Validate required fields
                if (!sectionData.original || !sectionData.act_id) {
                        console.error('Missing required fields in section data:', {
                                has_original: !!sectionData.original,
                                has_act_id: !!sectionData.act_id
                        });
                }

                const { error } = await supabase
                        .from('sections')
                        .insert([{
                                act_id: actId,
                                section_content: sectionData.section_text,
                                section_title: sectionData.section_title,
                                country: 'US',
                                additional: {
                                        original_title: sectionData.original,
                                        act_identifier: sectionData.act_id
                                }
                        }]);

                if (error) throw error;
        } catch (error) {
                console.error('Error inserting section:', error);
                throw error;
        }
}

async function checkIfProcessed(siteMapUrl) {
        const { data, error } = await supabase
                .from('acts_sections_scraping_sitemap')
                .select('*')
                .eq('url', siteMapUrl)
                .eq('processed', true);
        if (error) {
                console.error('Error fetching data:', error);
                return false;
        }

        return data.length > 0; // Returns true if processed, false otherwise
}

async function upsertSiteMap(siteMapUrl) {
        const alreadyProcessed = await checkIfProcessed(siteMapUrl)
        console.log('alreadyProcessed', alreadyProcessed)
        if (alreadyProcessed) {
                return;
        }
        const { data, error } = await supabase
                .from('acts_sections_scraping_sitemap')
                .insert([
                        { url: siteMapUrl, processed: true, created_at: new Date() }
                ]);

        if (error) {
                console.error('Error upserting:', error);
        }
}



function sleep(time) {
        return new Promise(resolve => setTimeout(resolve, time));
}


const config = {
        maxSitemaps: 1,         // Maximum number of sitemaps to process (reduced for testing)
        maxEntriesPerSitemap: 4, // Maximum number of entries to process per sitemap (reduced for testing)
        batchSize: 20,          // Size of batches for processing
        requestInterval: 0,   // Delay in milliseconds between batches (1 second) - Adjust as needed
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
                url, status: "error", error: "Failed after multiple retries"
        }
}

// Extract act identifier from URL
function extractActIdentifierFromUrl(url) {
        const urlParts = url.trim().split('/');
        return urlParts[urlParts.length - 2]; // Last part of URL is typically the identifier
}

// Check if section exists based on the URL
async function checkSectionExistsByUrl(url) {
        try {
                const actIdentifier = extractActIdentifierFromUrl(url);
                console.log('actIdentifier', url, actIdentifier);
                const { data, error } = await supabase
                        .from('sections')
                        .select('section_id')
                        .contains('additional', { act_identifier: actIdentifier })
                        .single();

                if (error) {
                        if (error.code === 'PGRST116') {  // No rows returned
                                return false;
                        }
                        throw error;
                }
                return !!data;
        } catch (error) {
                console.error('Error checking existing section by URL:', error);
                return false; // Assume it doesn't exist if there's an error
        }
}

async function scrapFindLaws() {
        let siteMapResponse = await fetch(`${cloudFlareUrl}/sitemaps/acts`);
        let siteMapUrls = await siteMapResponse.json(); // Convert response to JSON



        for (let siteMapUrl of siteMapUrls) {

                // Skip sitemaps containing '/cfr/'
                if (siteMapUrl.includes('/cfr/')) {
                        console.log(`Skipping CFR sitemap: ${siteMapUrl}`);
                        continue;
                }

                const isProcessed = await checkIfProcessed(siteMapUrl)
                // console.log('haaaya' , isProcessed)
                if (isProcessed) {
                        continue;
                }

                let pageUrlRes = await fetch(`${cloudFlareUrl}/pages?sitemap=${siteMapUrl}`)
                let pageUrls = await pageUrlRes.json()

                //For testing limitting the pageUrls
                // pageUrls = pageUrls.slice(0, config.maxEntriesPerSitemap)

                //Processing the pageUrls
                for (let i = 0; i < pageUrls.length; i += config.batchSize) {
                        const batch = pageUrls.slice(i, i + config.batchSize);
                        const fetchPromises = batch.map(async (url, index) => {
                                try {
                                        // Check if section already exists before making API call
                                        const sectionExists = await checkSectionExistsByUrl(url);
                                        if (sectionExists) {
                                                console.log(`Section already exists, skipping: ${url}`);
                                                return { url, status: 'skipped', message: 'Section already exists' };
                                        }

                                        const fullUrl = `${cloudFlareUrl}/scrape/acts?url=${url}`;
                                        const response = await fetchWithRetry(fullUrl);
                                        if (response.ok) {
                                                return await response.json()
                                        } else {
                                                return {
                                                        url,
                                                        status: 'error',
                                                        error: `HTTP error! Status: ${response.status}`
                                                }
                                        }
                                } catch (err) {
                                        return { url, status: "error", error: err.message };
                                }
                        });

                        const results = await Promise.all(fetchPromises);
                        // console.log(results)
                        //Storing in DB
                        for (const result of results) {
                                // console.log('result', result)
                                if (result.status === 'error' || result.status === 'skipped') {
                                        console.log(result)
                                        continue;
                                }
                                try {
                                        // Insert act and get its ID
                                        const actId = await insertAct(result.act_name);
                                        // Insert section using the act ID
                                        await insertSection(actId, result);
                                } catch (error) {
                                        console.error('Error processing result:', error.message);
                                        console.log('Failed to insert section:', {
                                                result,
                                                missingFields: {
                                                        section_id: !result.section_id,
                                                        original: !result.original,
                                                        act_id: !result.act_id
                                                }
                                        });
                                }
                        }
                }

                //SiteMap is processed completely
                await upsertSiteMap(siteMapUrl)
                await sleep(20000)
        }


}


scrapFindLaws().then(() => console.log('done'))