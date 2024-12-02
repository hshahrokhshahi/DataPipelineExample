import openai


def generate_summary(retrieved_records):
    prompt = "Generate a summary of the following customer comments:\n"
    for record in retrieved_records:
        prompt += f"{record['comments']}\n"

    response = openai.Completion.create(
        engine="text-davinci-003",
        prompt=prompt,
        max_tokens=150
    )
    return response.choices[0].text.strip()


# Generate a summary based on retrieved records
summary = generate_summary([data[i] for i in similar_records[0]])
print(summary)
