import orjson
import shortuuid
import datetime
import csv
import pandas

# cp ./Fottea/AListOfFiles.csv oo42hcksBxZYAOjqwGWu-metadata
# cp ./SolenPapers/Metadata.csv oo42hcksBxZYAOjqwGWu-metadata
# cp ./VeterinarniMedicina/IndexedVeterinarniMedicina.csv oo42hcksBxZYAOjqwGWu-metadata
# cp ./ResAgrEng/ResearchInAgriculturalEngineering.csv oo42hcksBxZYAOjqwGWu-metadata
# cp ./SoilWatRes/SoilAndWaterResearch.csv oo42hcksBxZYAOjqwGWu-metadata
# cp ./CCCC/Archive_CCCC.csv oo42hcksBxZYAOjqwGWu-metadata
# cp ./CCCC/CCCC.csv oo42hcksBxZYAOjqwGWu-metadata
# cp ./HortSci/HorticulturalScience.xlsx oo42hcksBxZYAOjqwGWu-metadata
# cp ./PlantSoidEnv/PlantSoilEnvironment.xlsx oo42hcksBxZYAOjqwGWu-metadata
# cp ./AgicultEcon/AgricultEcon.xlsx oo42hcksBxZYAOjqwGWu-metadata
# cp ./CzechJFoodSci/CzechJFoodSci.xlsx oo42hcksBxZYAOjqwGWu-metadata
# cp ./PlantProtectSci/PlantProtectionScience.xlsx oo42hcksBxZYAOjqwGWu-metadata
# cp ./CzechJGenetPlantBreed/CzechJOfGeneticsAndPlantBreeding.xlsx oo42hcksBxZYAOjqwGWu-metadata
# cp ./JForSci/JForrestSci.xlsx oo42hcksBxZYAOjqwGWu-metadata
# cp ./BiomedPapersOlomouc/BioMedOl.xlsx oo42hcksBxZYAOjqwGWu-metadata
# cp ./CzechJAnimSci/CzechJournalOfAnimalScience.xlsx oo42hcksBxZYAOjqwGWu-metadata

timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

with open(f"aac/annas_archive_meta__aacid__czech_oo42hcks_records__{timestamp}--{timestamp}.jsonl", 'wb') as output_file_handle:
    def process_csv(filename, fileid, filename_field, skip_lines, encoding):
        with open(filename, 'r', encoding=encoding) as input_file:
            print(f"{filename} {fileid} ..")
            csv.register_dialect(fileid, delimiter=';')
            header_row = []
            for index, row_arr in enumerate(csv.reader(input_file, fileid)):
                if index < skip_lines:
                    continue
                if index == skip_lines:
                    header_row = row_arr
                    continue
                dict_row = dict(zip(header_row, row_arr))
                # print(f"{index=} {row_arr=} {dict_row=}")
                # if index > 5:
                #     break

                uuid = shortuuid.uuid()
                aac_record = {
                    "aacid": f"aacid__czech_oo42hcks_records__{timestamp}__{uuid}",
                    "metadata": {
                        "id": f"{fileid}_{index}",
                        "filename": dict_row[filename_field],
                        "record": dict_row,
                    },
                }
                output_file_handle.write(orjson.dumps(aac_record, option=orjson.OPT_APPEND_NEWLINE))
                output_file_handle.flush()

    process_csv('Metadata.csv', 'solen_papers', 'FileName', 0, 'utf-8')
    process_csv('AListOfFiles.csv', 'fottea', 'File-href', 0, 'cp852')
    process_csv('Archive_CCCC.csv', 'archive_cccc', 'File name', 4, 'utf-8')
    process_csv('CCCC.csv', 'cccc_csv', 'Filename', 0, 'utf-8')
    process_csv('IndexedVeterinarniMedicina.csv', 'veterinarni_medicina', 'PDF-href', 0, 'utf-8')
    process_csv('ResearchInAgriculturalEngineering.csv', 'research_in_agricultural_engineering', 'PDF-href', 0, 'cp852')
    process_csv('SoilAndWaterResearch.csv', 'soil_and_water_research', 'PDF-href', 0, 'utf-8')

    def process_xlsx(filename, fileid, filename_field):
        print(f"{filename} {fileid} ..")
        df = pandas.read_excel(filename)
        for index, row in df.iterrows():
            dict_row = row.to_dict()
            # print(f"{index=} {row_arr=} {dict_row=}")
            # if index > 5:
            #     break

            if filename_field not in dict_row:
                print(dict_row)

            uuid = shortuuid.uuid()
            aac_record = {
                "aacid": f"aacid__czech_oo42hcks_records__{timestamp}__{uuid}",
                "metadata": {
                    "id": f"{fileid}_{index}",
                    "filename": dict_row[filename_field],
                    "record": dict_row,
                },
            }
            output_file_handle.write(orjson.dumps(aac_record, option=orjson.OPT_APPEND_NEWLINE))
            output_file_handle.flush()

    process_xlsx('AgricultEcon.xlsx', 'agricult_econ', 'PDF-href')
    process_xlsx('BioMedOl.xlsx', 'biomed_papers_olomouc', 'PDF-link-href')
    process_xlsx('CzechJFoodSci.xlsx', 'czech_j_food_sci', 'PDF-href')
    process_xlsx('CzechJOfGeneticsAndPlantBreeding.xlsx', 'czech_j_of_genetics_and_plant_breeding', 'PDF-href')
    process_xlsx('CzechJournalOfAnimalScience.xlsx', 'czech_journal_of_animal_science', 'PDF-href')
    process_xlsx('HorticulturalScience.xlsx', 'horticultural_science', 'PDF-href')
    process_xlsx('JForrestSci.xlsx', 'j_forrest_sci', 'PDF-href')
    process_xlsx('PlantProtectionScience.xlsx', 'plant_protection_science', 'PDF-href')
    process_xlsx('PlantSoilEnvironment.xlsx', 'plant_soil_environment', 'PDF-href')



