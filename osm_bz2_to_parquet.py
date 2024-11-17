import bz2
import xml.etree.ElementTree as ET
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import argparse

def convert_xml_to_parquet(input_file, output_file, chunk_size):

    # Initialize list for storing each chunk of data
    data_chunk = []
    first_chunk = True  # To initialize the ParquetWriter only once
    writer = None
    row_counter = 0

    # Step 1: Open and stream the XML
    with bz2.open(input_file, "rt") as file:
        # Step 2: Initialize iterparse to stream through XML file
        for event, element in ET.iterparse(file, events=("end",)):
            # Process each "changeset" element
            if element.tag == 'changeset':
                # Extract attributes and nested tags
                record_data = {
                    'id': element.get('id'),
                    'created_at': element.get('created_at'),
                    'uid': element.get('uid'),
                    'user': element.get('user'),
                    'num_changes': element.get('num_changes'),
                    'min_lat': element.get('min_lat'),
                    'min_lon': element.get('min_lon'),
                    'max_lat': element.get('max_lat'),
                    'max_lon': element.get('max_lon'),

                    # Set default values for tags that might be missing in chunk
                    'created_by': '',
                    'imagery_used': '',
                    'host': '',
                    'changesets_count': '', # number of changesets the user has made before the current one
                    'hashtags': '',
                    # 'comment': None
                    
                }
                
                for tag in element.findall('tag'):
                    if tag.get('k') == 'created_by':
                        record_data['created_by'] = tag.get('v')
                    elif tag.get('k') == 'imagery_used':
                        record_data['imagery_used'] = tag.get('v')
                    elif tag.get('k') == 'host':
                        record_data['host'] = tag.get('v')
                    elif tag.get('k') == 'changesets_count':
                        record_data['changesets_count'] = tag.get('v')
                    elif tag.get('k') == 'hashtags':
                        record_data['hashtags'] = tag.get('v')
                
                # Add record to the current chunk
                data_chunk.append(record_data)
                
                # Clear the processed element to free up memory
                element.clear()
                
                # Step 3: If chunk size is reached, write the chunk to Parquet
                if len(data_chunk) >= chunk_size:
                    # print(f"Writing chunk of size {len(data_chunk)} to Parquet.")
                    df_chunk = pd.DataFrame(data_chunk)
                    table = pa.Table.from_pandas(df_chunk)

                    if writer is None:
                        writer = pq.ParquetWriter(output_file, table.schema.remove_metadata())
                    
                    # Write or append the chunk to the Parquet file
                    if first_chunk:
                        # Initialize Parquet file with the first chunk
                        writer.write_table(table)
                        first_chunk = False
                    else:
                        # Append subsequent chunks
                        # with pq.ParquetWriter(output_file, table.schema) as writer:
                        writer.write_table(table)
                    
                    # Clear the chunk data
                    data_chunk = []
                    # writer.close()
                    # writer = None
            
                row_counter += 1
                if row_counter % 1000000 == 0:
                    print(f"Processed {row_counter//1000000} mln changesets")


        # Step 4: Write any remaining rows after loop ends
        if data_chunk:
            print(f"Writing final chunk of size {len(data_chunk)} to Parquet.")
            df_chunk = pd.DataFrame(data_chunk)
            table = pa.Table.from_pandas(df_chunk)
            if writer is None:
                writer = pq.ParquetWriter(output_file, table.schema.remove_metadata())
            writer.write_table(table)

            if writer is not None:
                writer.close()

            
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert large XML (bz2) to Parquet format with chunking.")
    parser.add_argument("-in", "--input", required=True, help="Path to the input .bz2 XML file")
    parser.add_argument("-out", "--output", required=True, help="Path to the output Parquet file")
    parser.add_argument("-chunk", "--chunk_size", type=int, default=1000, help="Number of records per chunk (default: 1000)")

    args = parser.parse_args()

    convert_xml_to_parquet(args.input, args.output, args.chunk_size)
