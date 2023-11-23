import os
import xlrd2


def excel_to_yaml(excel_path: str, yaml_path: str, card_col: int, name: str):
    wb = xlrd2.open_workbook(filename=excel_path)
    table = wb.sheet_by_name("Sheet1")
    num_rows = table.nrows
    card_set = set()

    if os.path.exists(yaml_path):
        os.remove(yaml_path)
    with open(yaml_path, mode="w") as f:
        f.write("cards_conf:\n")
        f.write("  - file_name: \"/data_path/%s.txt\"\n" % name)
        f.write("    cards:\n")
        for row_index in range(num_rows):
            if row_index == 0:
                continue
            card_id = table.cell_value(rowx=row_index, colx=card_col)
            if card_id in card_set:
                continue
            card_set.add(card_id)
            project = "icu30"
            media_name = "frame_sensor_data"
            # print("%d\t%s\t%s\t%s" % (row_index, card_id, project, media_name))
            f.write("      - card_id: %s\n" % card_id)
            f.write("        project: %s\n" % project)
            f.write("        media_name: %s\n" % media_name)
        
        
if __name__ == '__main__':
    name = "pers"
    card_col = 11
    
    excel_path = "/root/Data_Hospital_II/data/excel/%s.xlsx" % (name)
    yaml_path = "/root/Data_Hospital_II/data/yaml/%s_%d.yaml" % (name, card_col)
    excel_to_yaml(excel_path=excel_path, yaml_path=yaml_path, card_col=card_col, name=name)