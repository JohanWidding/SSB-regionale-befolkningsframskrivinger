import pandas as pd
import json

class JSONStat2Handler:
    def __init__(self, json_stat_data):
        """
        Initialize the JSONStat2Handler with JSON-stat 2 data.

        Parameters:
        - json_stat_data (dict): JSON-stat 2 data in dictionary format.
        """
        self.json_stat_data = json_stat_data

    def print_structure(self):
        """
        Prints the structure of the JSON-stat 2 data with all keys and their respective structures, including dimensions and their categories.
        """

        print("JSON-stat 2 Data Structure:")
        for key, value in self.json_stat_data.items():
            if key == "dimension":
                print(f"'{key}': {{")
                for dim_name, dim_details in value.items():
                    categories = dim_details.get("category", {}).get("label", {})
                    print(f"  '{dim_name}': [{', '.join(categories.keys())}],")
                print("},")
            else:
                # Print the structure of other keys with summaries
                if isinstance(value, dict):
                    print(f"'{key}': {{...}},")
                elif isinstance(value, list):
                    print(f"'{key}': [List of {len(value)} items],")
                else:
                    print(f"'{key}': {value},")


    def jsonstat_to_merged_dataframe(self, time_column, primary_category, secondary_category, columns_to_merge, value_column="value"):
        """
        Processes JSON-stat 2 data into a clean DataFrame, correctly mapping indices from the JSON `value` array
        to their respective dimensions and categories.

        Parameters:
        - time_column (str): The name of the time column to use as the index (e.g., "Tid").
        - primary_category (str): The primary category column to use for grouping (e.g., "Region").
        - secondary_category (str): The secondary category column to divide data by (e.g., "ContentsCode").
        - columns_to_merge (list): Columns to merge into a single category (e.g., ["Kjonn", "Alder"]).
        - value_column (str, optional): The name of the column containing values to summarize (default is "value").

        Returns:
        - pd.DataFrame: A summarized DataFrame with the time column as index, and merged categories as columns.
        """
        import pandas as pd
        import itertools

        # Extract dimensions and their sizes
        dimensions = self.json_stat_data["dimension"]
        sizes = self.json_stat_data["size"]

        # Prepare a list of all dimension names and their categories
        dimension_names = list(dimensions.keys())
        dimension_indices = [dimensions[dim]["category"]["index"] for dim in dimension_names]
        dimension_labels = {dim: dimensions[dim]["category"]["label"] for dim in dimension_names}

        # Create all combinations of indices
        index_combinations = list(itertools.product(*dimension_indices))

        # Map `value` array to DataFrame
        values = self.json_stat_data.get("value", [])
        data = []
        for idx, combination in enumerate(index_combinations):
            entry = {dimension_names[i]: dimension_labels[dimension_names[i]].get(str(combination[i]), combination[i])
                    for i in range(len(combination))}
            entry[value_column] = values[idx]
            data.append(entry)

        df = pd.DataFrame(data)

        # Ensure proper data types
        df[time_column] = pd.to_numeric(df[time_column], errors="coerce")
        if value_column in df.columns:
            df[value_column] = pd.to_numeric(df[value_column], errors="coerce")

        # Merge specified columns into a single category
        if columns_to_merge:
            df["MergedCategory"] = df[columns_to_merge].astype(str).agg("_".join, axis=1)

        # Group by the desired structure and summarize values
        group_by_columns = [time_column, primary_category, secondary_category]
        summary_df = (
            df.groupby(group_by_columns)[value_column]
            .sum()
            .unstack([primary_category, secondary_category])
            .fillna(0)
        )

        # Flatten the multi-index columns
        summary_df.columns = [
            f"{region}_{contents_code}" for region, contents_code in summary_df.columns
        ]

        # Set the time column as the index
        summary_df.index.name = time_column

        return summary_df




# Example usage:
# json_stat_data = {...}  # Your JSON-stat 2 data
# handler = JSONStat2Handler(json_stat_data)
# handler.print_structure()
# df = handler.process_to_dataframe("Tid", "Region", "ContentsCode", ["Kjonn", "Alder"])
# print(df)
