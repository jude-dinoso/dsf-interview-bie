Project Documentation

This document provides an overview of assumptions, limitations, review points, actions taken, and remaining tasks related to the development and processing of the dataset.

Assumptions

	•	Denormalized Dataset Structure: Datasets are assumed to be denormalized, and a strict data modeling schema (such as star or snowflake schema) is not enforced. The data transformations operate directly on this denormalized structure.
	•	Orchestration and ETL Validation Scope: Orchestration, error handling, and validation within ETL processes are outside the scope of this exercise.

Limitations

	•	Use Case: This dataset and its transformations are designed solely for use in fraud detection models, with no other use cases in mind.
	•	Session Definition: The definition of a session has been pre-determined and agreed upon, so any modifications to session logic are out of scope.

Review Points

	1.	Session Statistics Table Partitioning:
	•	The session statistics table is currently partitioned by session year, which may not be an optimal partitioning strategy for effective data retrieval and read optimization.
	•	Potential Impact: Upstream jobs that depend on this table may encounter longer read times due to the coarse partitioning.
	2.	Transformation Readability and Organization:
	•	Many transformations are grouped together, making them difficult to interpret and maintain.
	•	Suggestion: Column transformations with dependencies on one another should be modularized into functions that return only the required columns. This approach would enhance readability, maintainability, and scalability.
	3.	Data Reusability and Validation:
	•	The current denormalized dataset is not retained for reuse or validation purposes, which may hinder validation workflows and reduce efficiency in downstream processes.
	•	Key Data Points: Columns such as page_stay_duration and session_id, which are critical for session analytics and could support further aggregations, should ideally be available from the time the app_events table is created.
	4.	Insufficient Documentation:
	•	The transformations lack sufficient documentation. Clearer documentation would improve understanding of transformation logic and expected outputs, benefiting both maintenance and future development.
	5.	Lack of Unit Testing:
	•	No unit tests have been implemented to validate transformations, which could lead to inconsistencies and hinder the debugging process.

Actions Taken

	•	Enhanced Partitioning:
	•	Modified the session statistics table to be partitioned by event_date instead of session_year, resulting in more efficient data access for downstream processes.
	•	Refactored Transformations:
	•	Refactored transformation logic to be more modular and easier to follow. Additional columns can now be easily added as static methods within the class. Future transformation sets will be placed in separate files for clearer organization.
	•	Improved Documentation:
	•	Added comment blocks to explain complex transformations, clarifying their logic and aiding future maintenance.

Remaining TODOs

	•	Develop Unit Testing Framework:
	•	Implement a unit testing framework to support Continuous Integration/Continuous Deployment (CI/CD) pipelines.
	•	Unit tests will ensure data consistency, validate transformation outputs, and catch errors early in the development process.

This README serves as a guide to the current structure, recent improvements, and future areas for development within this project.