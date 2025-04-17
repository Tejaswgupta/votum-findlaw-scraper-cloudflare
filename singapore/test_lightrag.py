import os
import asyncio
import json
from lightrag import LightRAG, QueryParam
from lightrag.kg.shared_storage import initialize_pipeline_status
from lightrag.llm.openai import openai_complete_if_cache, openai_embed
from lightrag.utils import EmbeddingFunc
import numpy as np
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()


WORKING_DIR = "./singapore_acts"

if not os.path.exists(WORKING_DIR):
    os.mkdir(WORKING_DIR)


async def llm_model_func(
    prompt, system_prompt=None, history_messages=[], **kwargs
) -> str:
    return await openai_complete_if_cache(
        # "mistral-small-latest",
        "DeepSeek-V3",
        prompt,
        system_prompt=system_prompt,
        history_messages=history_messages,
        api_key="sk-ncjndicw88924bb2",
        # api_key='sk-ZhkEF1NJlGpWmn5lo-3Qaw',
        base_url="https://model.thevotum.com",
        **kwargs,
    )


async def embedding_func(texts: list[str]) -> np.ndarray:
    return await openai_embed(
        texts,
        model="cohere-embed-v3",
        api_key="sk-ncjndicw88924bb2",
        base_url="https://model.thevotum.com",
    )


async def get_embedding_dim():
    test_text = ["This is a test sentence."]
    embedding = await embedding_func(test_text)
    embedding_dim = embedding.shape[1]
    return embedding_dim


# function test
async def test_funcs():
    result = await llm_model_func("How are you?")
    print("llm_model_func: ", result)

    result = await embedding_func(["How are you?"])
    print("embedding_func: ", result)


if not os.path.exists(WORKING_DIR):
    os.mkdir(WORKING_DIR)


async def initialize_rag():
    embedding_dimension = await get_embedding_dim()
    print(f"Detected embedding dimension: {embedding_dimension}")

    rag = LightRAG(
        working_dir=WORKING_DIR,
        llm_model_func=llm_model_func,
        embedding_func=EmbeddingFunc(
            embedding_dim=embedding_dimension,
            max_token_size=8192,
            func=embedding_func,
        ),
    )

    await rag.initialize_storages()
    await initialize_pipeline_status()

    return rag

async def main():
    rag = await initialize_rag()
    
    # with open('sections.json', 'r') as file:
    #     sections = json.load(file)

    supabase = create_client(
        os.getenv("SUPABASE_URL"),
        os.getenv("SUPABASE_KEY"),
    )

    # sections = supabase.table("sections").select("*").eq("act_id",3547).execute().data
    # print(len(sections))

    # for section in sections:
    #     act_name = 'ACCOUNTANTS ACT 2004'
    #     # subsidiary_legislation = 'Subsidiary Legislation Name: ACCOUNTANTS\n(PRESCRIBED DOCUMENTS AND\nINFORMATION) RULES 2024'
    #     text = f"""
    #     Act Name: {act_name}
    #     Section Title: {section['section_title']}
    #     Section Content: {section['section_content']}
    #     """
    #     await rag.ainsert(text)


    query = "A client needs a certified copy of their accounting firm’s approval document but accidentally included their personal mobile number in the filing."
    result = await rag.aquery(
        query,
        QueryParam(
            mode='mix',
            top_k=10,
        )
    )
    print(result)

if __name__ == "__main__":
    asyncio.run(main())


