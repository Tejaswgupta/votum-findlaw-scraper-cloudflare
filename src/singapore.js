import welcome from "welcome.html";

/**
 * @typedef {Object} Env
 */

export default {
    /**
     * @param {Request} request
     * @param {Env} env
     * @param {ExecutionContext} ctx
     * @returns {Promise<Response>}
     */
    async fetch(request, env, ctx) {
        const url = new URL(request.url);
        console.log(`Hello ${navigator.userAgent} at path ${url.pathname}!`);

        if (url.pathname === "/api/sitemap/cases") {
            // Get the index from query parameters, default to 1 if not provided
            const index = parseInt(url.searchParams.get('index') || '1', 10);
            // You could also call a third party API here
            const text = await getCaseLawsList(index);
            return Response.json(text);
        }
        else if (url.pathname === "/api/scrape/cases") {
            const caseUrl = url.searchParams.get('url');
            if (!caseUrl) {
                return Response.json({ error: "URL parameter is required" }, { status: 400 });
            }
            const text = await scrapeCaseLaw(caseUrl);
            return Response.json(text);
        }
        return new Response(welcome, {
            headers: {
                "content-type": "text/html",
            },
        });
    },
};


async function getCaseLawsList(index = 1) {
    const response = await fetch(`https://www.elitigation.sg/gd/Home/Index?Filter=SUPCT&YearOfDecision=All&SortBy=DateOfDecision&CurrentPage=${index}&SortAscending=False&PageSize=0&Verbose=False&SearchQueryTime=0&SearchTotalHits=0&SearchMode=True&SpanMultiplePages=False`, {
        headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://sso.agc.gov.sg/',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1'
        },
        // Cloudflare-specific options
        cf: {
            cacheTtl: 3600,
            cacheEverything: true
        }
    });

    const pageData = await response.text();

    const selector = '.h5.gd-heardertext'; // Updated selector to target a tags

    // Create an array to store extracted hrefs
    const extractedLinks = [];

    // Use HTMLRewriter to parse content
    const rewriter = new HTMLRewriter()
        .on(selector, {
            element(element) {
                const href = element.getAttribute('href');
                if (href) {
                    extractedLinks.push(href);
                }
            }
        });

    // Transform the response
    const transformed = rewriter.transform(new Response(pageData));

    // Wait for all content to be processed
    await transformed.text();

    // Return the extracted links
    return extractedLinks;
}

async function scrapeCaseLaw(caseUrl) {
    try {
        // Fetch case data from the provided URL
        const response = await fetch(caseUrl, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
            },
            cf: {
                cacheTtl: 3600,
                cacheEverything: true
            }
        });

        const html = await response.text();

        // Create an object to store the extracted information
        const caseInfo = {
            caseName: '',
            caseNo: '',
            citationNo: '',
            courtName: '',
            caseText: '', // Added field for case text/transcript
            judgmentDate: '', // Added field for judgment date
        };

        // Use HTMLRewriter to extract the information
        const rewriter = new HTMLRewriter()
            // Extract case name
            .on('div.HN-CaseName', {
                text(text) {
                    // Accumulate all text from HN-CaseName elements
                    caseInfo.caseName = caseInfo.caseName + " " + text.text;
                }
            })
            // Extract citation number
            .on('div.HN-NeutralCit', {
                text(text) {
                    caseInfo.citationNo += text.text;
                }
            })
            // Extract case number from CaseNumber div
            .on('div.CaseNumber', {
                text(text) {
                    caseInfo.caseNo += text.text;
                }
            })
            // Extract court name from multiple possible locations
            .on('div.title', {
                text(text) {
                    caseInfo.courtName += text.text + " ";
                }
            })
            // Extract judgment date from HN-DateOfJudgment or date fields
            .on('div.Judg-Date-Reserved', {
                text(text) {
                    const cleanText = text.text.split('&emsp;')[0].trim();
                    console.log(cleanText);
                    caseInfo.judgmentDate += cleanText;
                }
            })
            // Extract case text/transcript from the second row inside divJudgement
            .on('div#divJudgement > content > div.row:nth-child(4)', {
                text(text) {
                    caseInfo.caseText += text.text;
                }
            });

        // Transform the response
        const transformed = rewriter.transform(new Response(html));

        // Wait for all content to be processed
        await transformed.text();

        const parsedDate = new Date(caseInfo.judgmentDate);
        console.log(parsedDate);

        return {
            case_name: caseInfo.caseName.trim(),
            case_no: caseInfo.caseNo.trim(),
            citation: caseInfo.citationNo.trim(),
            court_name: caseInfo.courtName.trim(),
            case_text: caseInfo.caseText.trim(),
            date: parsedDate
        }
    } catch (error) {
        console.error('Error scraping case law:', error);
        return { error: error.message };
    }
}

/// https://votum-scraper-singapore.tejasw.workers.dev/api/scrape/cases?url=https://www.elitigation.sg/gd/s/2025_SGHC_27