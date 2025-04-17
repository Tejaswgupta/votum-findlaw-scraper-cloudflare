import traceback
from dotenv import load_dotenv
from supabase import create_client
import os
import asyncio
import time
import sys



load_dotenv()

supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))


sys.path.append('/home/azureuser')
from votum_fastapi.oai.legal_summary import generate_legal_summary

async def summarize(case_text: str) -> str|None:
    try:
        result = await generate_legal_summary(case_text)
        return result
    except asyncio.TimeoutError:
        print("Request timed out while generating summary")
        return None
    except Exception as e:
        print(f"Error in summarize function: {str(e)}")
        return None

async def process_case(case):
    try:
        case_id = case["id"]
        case_content = case["case_text"]
        if case_content is None or case_content.strip() == "":
            print(f"Skipping case {case_id} because it has no content")
            return
        
        # Get summary
        start_time = time.time()
        print(f"Summarizing case {case_id}, court: {case['standard_court_name']}")
        summary = await summarize(case_content)
        elapsed = time.time() - start_time

        if summary is None:
            print(f"Skipping case {case_id} because it has no summary")
            return
        
        # Update the database
        supabase.table("caselaw_singapore").update({"summary": summary}).eq("id", case_id).execute()
        
        print(f"Updated summary for case {case_id} (API call: {elapsed:.2f}s)")
    except Exception as e:
        traceback.print_exc()
        print(f"Error processing case {case['id']}: {str(e)}")

async def process_batch(batch):
    tasks = [process_case(case) for case in batch]
    await asyncio.gather(*tasks)
    
async def process_cases():
    # Fetch cases without summaries
    result = supabase.table("caselaw_singapore")\
        .select("id", "case_text, standard_court_name")\
        .is_("summary", None)\
        .neq("case_text", "")\
        .not_.is_("standard_court_name", None)\
        .limit(100)\
        .execute()

    cases = result.data
    print(f"Processing {len(cases)} cases")
    
    if not cases:
        return False  # No more cases to process

    # Process cases in batches of 10
    batch_size = 5
    for i in range(0, len(cases), batch_size):
        batch = cases[i:i+batch_size]
        print(f"Processing batch {i//batch_size + 1}/{(len(cases)-1)//batch_size + 1} with {len(batch)} cases")
        await process_batch(batch)
    
    return True  # More cases might be available

async def main():
    print("Starting summarization")
    
    try:
        while True:
            has_more = await process_cases()
            
            if not has_more:
                print("No more cases to process")
                break
    except Exception as e:
        traceback.print_exc()
        print(f"Error in main processing loop: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())