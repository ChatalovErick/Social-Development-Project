[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data]{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Reference_Database age_dimension_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Reference_Database age_dimension" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Reference_Database.age_dimension", ItemKind = "Table"]}[Data],
  #"Marked key columns" = Table.AddKey(#"Navigation 2", {"Code_of_Age"}, false)
in
  #"Marked key columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Reference_Database area_dimension_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Reference_Database area_dimension" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Reference_Database.area_dimension", ItemKind = "Table"]}[Data],
  #"Marked key columns" = Table.AddKey(#"Navigation 2", {"Area_Code"}, false)
in
  #"Marked key columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Reference_Database dim_geography_m49_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Reference_Database dim_geography_m49" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Reference_Database.dim_geography_m49", ItemKind = "Table"]}[Data],
  #"Reordered columns" = Table.ReorderColumns(#"Navigation 2", {"country_or_area", "iso-alpha2_code", "m49_code", "iso-alpha3_code", "global_code", "global_name", "region_code", "region_name", "sub-region_code", "sub-region_name", "intermediate_region_code", "intermediate_region_name", "least_developed_countries_ldc", "land_locked_developing_countries_lldc", "small_island_developing_states_sids"}),
  #"Marked key columns" = Table.AddKey(#"Reordered columns", {"country_or_area"}, false),
  #"Removed columns" = Table.RemoveColumns(#"Marked key columns", {"global_code", "global_name", "least_developed_countries_ldc", "land_locked_developing_countries_lldc", "small_island_developing_states_sids"}),
  #"Marked key columns 1" = Table.AddKey(#"Removed columns", {"iso-alpha2_code"}, false),
  #"Marked key columns 2" = Table.AddKey(#"Marked key columns 1", {"m49_code"}, false),
  #"Marked key columns 3" = Table.AddKey(#"Marked key columns 2", {"iso-alpha3_code"}, false)
in
  #"Marked key columns 3";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Reference_Database education_attainment_15plus_dimension_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Reference_Database education_attainment_15plus_dimension" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Reference_Database.education_attainment_15plus_dimension", ItemKind = "Table"]}[Data],
  #"Marked key columns" = Table.AddKey(#"Navigation 2", {"Code_of_Educational_attainment"}, false)
in
  #"Marked key columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Reference_Database employment_status_dimension_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Reference_Database employment_status_dimension" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Reference_Database.employment_status_dimension", ItemKind = "Table"]}[Data],
  #"Marked key columns" = Table.AddKey(#"Navigation 2", {"Code_of_Status_in_employment"}, false)
in
  #"Marked key columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Reference_Database school_attendance_dimension_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Reference_Database school_attendance_dimension" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Reference_Database.school_attendance_dimension", ItemKind = "Table"]}[Data],
  #"Marked key columns" = Table.AddKey(#"Navigation 2", {"Code_of_School_attendance"}, false)
in
  #"Marked key columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Reference_Database sex_dimension_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Reference_Database sex_dimension" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "2040f8e7-720b-4901-acd5-9b9c700b12af"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Reference_Database.sex_dimension", ItemKind = "Table"]}[Data],
  #"Marked key columns" = Table.AddKey(#"Navigation 2", {"Sex_Code"}, false)
in
  #"Marked key columns";
shared #"Reference_Database age_dimension_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "e003063b-04a8-42a0-8e85-b0243d356adb"]}[Data],
  TableNavigation = Navigation_2{[Id = "Dim_Database age_dimension", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"Reference_Database area_dimension_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "e003063b-04a8-42a0-8e85-b0243d356adb"]}[Data],
  TableNavigation = Navigation_2{[Id = "Dim_Database area_dimension", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"Reference_Database dim_geography_m49_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "e003063b-04a8-42a0-8e85-b0243d356adb"]}[Data],
  TableNavigation = Navigation_2{[Id = "Dim_Database dim_geography_m49", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"Reference_Database education_attainment_15plus_dimension_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "e003063b-04a8-42a0-8e85-b0243d356adb"]}[Data],
  TableNavigation = Navigation_2{[Id = "Dim_Database education_attainment_15plus_dimension", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"Reference_Database employment_status_dimension_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "e003063b-04a8-42a0-8e85-b0243d356adb"]}[Data],
  TableNavigation = Navigation_2{[Id = "Dim_Database employment_status_dimension", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"Reference_Database school_attendance_dimension_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "e003063b-04a8-42a0-8e85-b0243d356adb"]}[Data],
  TableNavigation = Navigation_2{[Id = "Dim_Database school_attendance_dimension", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared #"Reference_Database sex_dimension_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d83c184e-82f0-4705-952c-0e29c5cb5274"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "e003063b-04a8-42a0-8e85-b0243d356adb"]}[Data],
  TableNavigation = Navigation_2{[Id = "Dim_Database sex_dimension", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
