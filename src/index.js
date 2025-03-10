import { createClient } from '@supabase/supabase-js';

// Create a single supabase client for interacting with your database
const supabase = createClient('https://supabase.thevotum.com', "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.ewogICJyb2xlIjogImFub24iLAogICJpc3MiOiAic3VwYWJhc2UiLAogICJpYXQiOiAxNzE3OTU3ODAwLAogICJleHAiOiAxODc1NzI0MjAwCn0.XrCbkNQDLY0fvtqJ7ZHdimDSihI7sRfbqtIjqOXgrNg")

// Function to check if act exists and get its ID
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
		const { error } = await supabase
			.from('sections')
			.insert([{
				act_id: actId,
				section_title: sectionData.section_title,
				section_content: sectionData.section_text,
				country : 'US',
				additional: {
					chapter_no: sectionData.chapter_no,
					section_id: sectionData.section_id,
					original_title: sectionData.original,
					act_identifier: sectionData.act_id
				}
			}]);

		if (error) throw error;
	} catch (error) {
		// console.error('Error inserting section:', error);
		throw error;
	}
}

export default {
	async fetch(request, env, ctx) {
		// Configuration for testing and rate limiting
		const config = {
			maxSitemaps: 10,         // Maximum number of sitemaps to process (reduced for testing)
			maxEntriesPerSitemap: 100, // Maximum number of entries to process per sitemap (reduced for testing)
			batchSize: 1,          // Size of batches for processing
			requestInterval: 2000,   // Delay in milliseconds between batches (1 second) - Adjust as needed
		};
		// Function to extract URLs from XML content using regex
		function extractUrlsFromXml(xmlText) {
			const locRegex = /<loc>\s*(.*?)\s*<\/loc>/g;
			const matches = [...xmlText.matchAll(locRegex)];
			return matches.map(match => match[1].trim());
		}

		// Function to get URLs from a sitemap XML
		async function getSitemapUrls(sitemapUrl) {
			const response = await fetch(sitemapUrl);
			const xmlText = await response.text();
			return extractUrlsFromXml(xmlText);
		}

		// Async sleep function for rate limiting
		function sleep(ms) {
			return new Promise(resolve => setTimeout(resolve, ms));
		}

		try {
			// Fetch the main sitemap index
			const sitemapIndexUrl = "https://codes.findlaw.com/sitemapindex.xml";
			let sitemapUrls = await getSitemapUrls(sitemapIndexUrl);

			// Limit the number of sitemaps for testing 
			// sitemapUrls = sitemapUrls.slice(0, config.maxSitemaps);
			// console.log(`Processing ${sitemapUrls.length} sitemaps`);

			let allResults = [];

			// Process each sitemap
			for (const sitemapUrl of sitemapUrls) {
				// console.log(`Processing sitemap: ${sitemapUrl}`);

				// Get all page URLs from this sitemap
				let pageUrls = await getSitemapUrls(sitemapUrl);
				// Limit the number of entries per sitemap for testing
				// pageUrls = pageUrls.slice(0, config.maxEntriesPerSitemap);
				// console.log(`Processing ${pageUrls.length} entries from sitemap`);
				
				// Process each page URL in batches
				for (let i = 0; i < pageUrls.length; i += config.batchSize) {
					const batch = pageUrls.slice(i, i + config.batchSize);
					const fetchPromises = batch.map(async (url) => {
						try {
							const response = await fetch(url);
							if (response.ok) {
								const html = await response.text();
								const extractedText = extractMeaningfulContent(html, url);
								return { url, status: "success", content: extractedText };
							} else {
								return { url, status: "error", error: `HTTP ${response.status}` };
							}
						} catch (err) {
							return { url, status: "error", error: err.message };
						}
					});

					const results = await Promise.all(fetchPromises);
					// Process results for database insertion
					for (const result of results) {
						console.log('result' , result)
						if (result.status === 'success') {
							try {
								// Insert act and get its ID
								const actId = await insertAct(result.content.act_name);
								
								// Insert section using the act ID
								await insertSection(actId, result.content);
							} catch (error) {
								// console.error('Error processing result:', error);
								result.status = 'error';
								result.error = error.message;
							}
						}
					}
					
					allResults = allResults.concat(results);

					// Rate limiting: Wait for the specified interval before processing the next batch
					if (i + config.batchSize < pageUrls.length) { // Don't sleep after the last batch
						console.log(`Waiting for ${config.requestInterval}ms before next batch...`);
						await sleep(config.requestInterval);
					}
				}
			}

			// Return all processed results
			return new Response(JSON.stringify({
				config: {
					maxSitemaps: config.maxSitemaps,
					maxEntriesPerSitemap: config.maxEntriesPerSitemap,
					totalSitemapsProcessed: sitemapUrls.length,
					totalEntriesProcessed: allResults.length,
					requestInterval: config.requestInterval
				},
				results: allResults
			}, null, 2), {
				headers: { "Content-Type": "application/json" },
			});
		} catch (error) {
			return new Response(JSON.stringify({ error: error.message }), {
				status: 500,
				headers: { "Content-Type": "application/json" },
			});
		}
	}
}
function extractActDetails(text) {
	/**
	 * Extracts the act name, chapter number, section ID, and section title from a given text.
	 *
	 * @param {string} text The input string containing the act details.
	 * @returns {object|null} An object containing the extracted components, or null if the format doesn't match.
	 */

	// Improved regex to handle variations, including cases with letters in section ID

	const match = text.match(/(.+?)\s*(\(Ch\.?\s*[\w\-\.\s]+\))?\s*§\s*([\w\.\-]+)\.?\s*(.*)/i);

	if (match) {
		const actName = match[1] ? match[1].trim() : null;
		const chapterNo = match[2] ? match[2].trim() : null;
		const sectionId = match[4] ? match[4].trim() : null; // Use group 4 for section ID
		const sectionTitle = match[5] ? match[5].trim() : null;

		return {
			act_name: actName,
			chapter_no: chapterNo,
			section_id: sectionId,
			section_title: sectionTitle,
			original: text,
		};
	}
	return null;
}

function extractMeaningfulContent(html, url) {
	/**
	 * Extracts meaningful content from HTML, representing a single section.
	 *
	 * @param {string} html The HTML content as a string.
	 * @param {string} url The URL of the page.
	 * @returns {object} An object representing the section with act details and text.
	 */

	// --- 1. Extract Overall Act Information (from <h1>) ---
	const titleMatch = html.match(/<h1>(.*?)<\/h1>/i);
	const title = titleMatch ? titleMatch[1] : "No title found";

	let actDetails = extractActDetails(title);
	if (!actDetails) {
		actDetails = {  // Fallback if title parsing fails
			act_name: "No Act Name Found",
			chapter_no: null,
			section_id: null,
			section_title: null,
			original: null,
		};

	}

	// --- 2. Extract Act ID (from URL) ---
	const actIdMatch = url.match(/\/([a-z0-9\-]+)\/$/i);
	const actId = actIdMatch ? actIdMatch[1] : "No Act ID found";

	// --- 3. Extract and Process Section Text (No Subsections) ---
	// Remove the <h1> tag and its content
	// Extract all sections (subsections) of the body content
	const subsectionsMatches = [...html.matchAll(/<div class="codes-content__text codes-content__text--min-height">([\s\S]*?)<\/div>/g)];

	// Extract and clean up all subsection text
	const subsections = subsectionsMatches.map(subsection => {
		const textContent = subsection[1].replace(/<[^>]+>/g, '').trim();  // Remove HTML tags and clean the text
		return textContent;
	});


	const sectionData = {
		act_name: actDetails.act_name,
		chapter_no: actDetails.chapter_no,
		act_id: actId,
		section_id: actDetails.section_id,
		section_title: actDetails.section_title,
		section_text: subsections.join('\n'),
		original: actDetails.original

	};

	return sectionData;
}

// fetchData().then(() => console.log("Done!")).catch(console.error);