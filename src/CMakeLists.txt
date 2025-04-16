
find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

add_executable(
    ${PROJECT_NAME}
    main.cpp
)

target_link_libraries(
    ${PROJECT_NAME}
    PRIVATE
    Arrow::arrow_shared
    Parquet::parquet_shared
)
