# Project Documentation

This document provides an overview of assumptions, limitations, review points, actions taken, and remaining tasks related to the processing and transformation of the dataset.

---

## Assumptions

- **Denormalized Dataset Structure**:  
  The datasets are assumed to be denormalized, and strict data modeling schemas (such as star or snowflake schema) are **not enforced**. The data transformations operate directly on this denormalized structure.

- **Orchestration and ETL Validation Scope**:  
  Orchestration, error handling, and validation within ETL processes are **out of scope** for this exercise.

---

## Limitations

- **Use Case**:  
  The dataset and its transformations are designed solely for use in fraud detection models, with no other use cases in mind.

- **Session Definition**:  
  The session definition has been pre-determined and agreed upon. Any modifications to session logic are outside the scope of this project.

---

## Review Points

1. **Session Statistics Table Partitioning**:  
   The session statistics table is currently partitioned by `session_year`, which may not be an optimal strategy for effective data retrieval and read optimization.

   - **Impact**: Upstream jobs that rely on this table may experience slower read times due to this coarse partitioning.

2. **Transformation Readability and Organization**:  
   Many transformations are grouped together, making the logic difficult to follow and maintain.

   - **Recommendation**: Modularize column transformations that depend on each other into separate functions that return only the required columns. This approach improves readability, maintainability, and scalability.

3. **Data Reusability and Validation**:  
   The current denormalized dataset is not retained for reuse or validation purposes, which can hinder validation workflows and reduce downstream process efficiency.

   - **Key Data Points**: Columns such as `page_stay_duration` and `session_id`, which are critical for session analytics and could support future aggregations, should ideally be made available when the `app_events` table is created.

4. **Documentation**:  
   The transformations lack sufficient documentation. Clearer documentation would improve the understanding of transformation logic and expected outputs, benefiting maintenance and future development.

5. **Lack of Unit Testing**:  
   No unit tests have been implemented to validate transformations, potentially leading to inconsistencies and making debugging more difficult.

---

## Actions Taken

- **Improved Partitioning**:  
   The session statistics table has been modified to partition by `event_date` instead of `session_year`, allowing for more efficient data access for downstream processes.

- **Refactored Transformations**:  
   Transformation logic has been reorganized to be more modular and easier to follow. Additional columns can now be added as static methods within the class. Future transformation sets will be organized in separate files for better structure.

- **Enhanced Documentation**:  
   Added comment blocks to explain complex transformations, clarifying logic and aiding in future maintenance.

---

## Remaining TODOs

- **Develop Unit Testing Framework**:  
   Implement a unit testing framework to support Continuous Integration/Continuous Deployment (CI/CD) pipelines. Unit tests will ensure data consistency, validate transformation outputs, and help catch errors early in the development process.

---
