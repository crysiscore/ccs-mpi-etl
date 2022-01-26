sql_query_mpi_location <- "select * from location;"

sql_query_mpi_patients <- "

SELECT * 
FROM 
(SELECT 	
            inicio_real.patient_id,
			CONCAT(pid.identifier,' ') AS NID,
            CONCAT(IFNULL(pn.given_name,''),' ',IFNULL(pn.middle_name,''),' ',IFNULL(pn.family_name,'')) AS 'NomeCompleto',
            pn.given_name,
            pn.middle_name,
			p.gender,
            pn.family_name,
			DATE_FORMAT(p.birthdate,'%d/%m/%Y') as birthdate ,
            ROUND(DATEDIFF(@endDate,p.birthdate)/365) idade_actual,
            DATE_FORMAT(inicio_real.data_inicio,'%d/%m/%Y') as data_inicio,
            pad3.county_district AS 'Distrito',
			pad3.address2 AS 'Padministrativo',
			pad3.address6 AS 'Localidade',
			pad3.address5 AS 'Bairro',
			pad3.address1 AS 'PontoReferencia',
            telef.value as telefone,
			IF(DATEDIFF(@endDate,visita.value_datetime)<=28,'ACTIVO EM TARV','ABANDONO NAO NOTIFICADO') estado,
            programa_tarv.state,            
            if(death.name is null ,'' , 1) as death,
            death.name as cause_of_death,
            DATE_FORMAT(death.obs_datetime,'%d/%m/%Y') as death_date,
            p.uuid
			
	FROM	
	(	SELECT patient_id,MIN(data_inicio) data_inicio
		FROM
			(	
			
				/*Patients on ART who initiated the ARV DRUGS: ART Regimen Start Date*/
				
						SELECT 	p.patient_id,MIN(e.encounter_datetime) data_inicio
						FROM 	patient p 
								INNER JOIN encounter e ON p.patient_id=e.patient_id	
								INNER JOIN obs o ON o.encounter_id=e.encounter_id
						WHERE 	e.voided=0 AND o.voided=0 AND p.voided=0 AND 
								e.encounter_type IN (18,6,9) AND o.concept_id=1255 AND o.value_coded=1256 AND 
								e.encounter_datetime<=@endDate AND e.location_id=@location
						GROUP BY p.patient_id
				
						UNION
				
						/*Patients on ART who have art start date: ART Start date*/
						SELECT 	p.patient_id,MIN(value_datetime) data_inicio
						FROM 	patient p
								INNER JOIN encounter e ON p.patient_id=e.patient_id
								INNER JOIN obs o ON e.encounter_id=o.encounter_id
						WHERE 	p.voided=0 AND e.voided=0 AND o.voided=0 AND e.encounter_type IN (18,6,9,53) AND 
								o.concept_id=1190 AND o.value_datetime IS NOT NULL AND 
								o.value_datetime<=@endDate AND e.location_id=@location
						GROUP BY p.patient_id

						UNION

						/*Patients enrolled in ART Program: OpenMRS Program*/
						SELECT 	pg.patient_id,MIN(date_enrolled) data_inicio
						FROM 	patient p INNER JOIN patient_program pg ON p.patient_id=pg.patient_id
						WHERE 	pg.voided=0 AND p.voided=0 AND program_id=2 AND date_enrolled<=@endDate AND location_id=@location
						GROUP BY pg.patient_id
						
						UNION
						
						
						/*Patients with first drugs pick up date set in Pharmacy: First ART Start Date*/
						  SELECT 	e.patient_id, MIN(e.encounter_datetime) AS data_inicio 
						  FROM 		patient p
									INNER JOIN encounter e ON p.patient_id=e.patient_id
						  WHERE		p.voided=0 AND e.encounter_type=18 AND e.voided=0 AND e.encounter_datetime<=@endDate AND e.location_id=@location
						  GROUP BY 	p.patient_id
					  

				
				
			) inicio
		GROUP BY patient_id	
	)inicio_real
		INNER JOIN person p ON p.person_id=inicio_real.patient_id
		
		LEFT JOIN 
			(	SELECT pad1.*
				FROM person_address pad1
				INNER JOIN 
				(
					SELECT person_id,MIN(person_address_id) id 
					FROM person_address
					WHERE voided=0
					GROUP BY person_id
				) pad2
				WHERE pad1.person_id=pad2.person_id AND pad1.person_address_id=pad2.id
			) pad3 ON pad3.person_id=inicio_real.patient_id				
			LEFT JOIN 			
			(	SELECT pn1.*
				FROM person_name pn1
				INNER JOIN 
				(
					SELECT person_id,MIN(person_name_id) id 
					FROM person_name
					WHERE voided=0
					GROUP BY person_id
				) pn2
				WHERE pn1.person_id=pn2.person_id AND pn1.person_name_id=pn2.id
			) pn ON pn.person_id=inicio_real.patient_id			
			LEFT JOIN
			(       SELECT pid1.*
					FROM patient_identifier pid1
					INNER JOIN
					(
						SELECT patient_id,MIN(patient_identifier_id) id
						FROM patient_identifier
						WHERE voided=0
						GROUP BY patient_id
					) pid2
					WHERE pid1.patient_id=pid2.patient_id AND pid1.patient_identifier_id=pid2.id
			) pid ON pid.patient_id=inicio_real.patient_id
		
  LEFT JOIN (
     SELECT o.person_id,o.concept_id, o.value_coded,o.value_coded_name_id,cn.name,  o.obs_datetime
  FROM obs o inner join concept_name cn on cn.concept_name_id= o.value_coded_name_id
 where o.concept_id=5002 and cn.locale='pt'  and o.voided=0
 ) death ON death.person_id=inicio_real.patient_id
/* ******************************* Telefone **************************** */
	LEFT JOIN (
		SELECT  p.person_id, p.value  
		FROM person_attribute p
     WHERE  p.person_attribute_type_id=9 
    AND p.value IS NOT NULL AND p.value<>'' AND p.voided=0 
	) telef  ON telef.person_id = inicio_real.patient_id
left join
(	SELECT ultimavisita.patient_id,ultimavisita.value_datetime,ultimavisita.encounter_type
			FROM
				(	SELECT 	p.patient_id,MAX(o.value_datetime) AS value_datetime, e.encounter_type 
					FROM 	encounter e 
					INNER JOIN obs o ON o.encounter_id=e.encounter_id 
					INNER JOIN patient p ON p.patient_id=e.patient_id 		
					WHERE 	e.voided=0 AND p.voided=0 and o.voided =0  AND e.encounter_type IN (6,9,18) AND  o.concept_id in (5096 ,1410)
						and	e.location_id=@location AND e.encounter_datetime <=@endDate  and o.value_datetime is  not null
					GROUP BY p.patient_id
				) ultimavisita

		) visita ON visita.patient_id=inicio_real.patient_id -- and DATEDIFF(@endDate,visita.value_datetime)<= 28
 left join (
			SELECT 	pg.patient_id		,
			case ps.state
            when 7 then  'TRANSFERIDO PARA'
            when 8 then  'SUSPENSO'
            when 9 then  'ABANDONO'
            when 10 then  'OBITOU'
            end as state
            
			FROM 	patient p 
					INNER JOIN patient_program pg ON p.patient_id=pg.patient_id
					INNER JOIN patient_state ps ON pg.patient_program_id=ps.patient_program_id
			WHERE 	pg.voided=0 AND ps.voided=0 AND p.voided=0 AND 
					pg.program_id=2 AND ps.state IN (7,8,9,10) AND 
					ps.end_date IS NULL AND location_id=@location AND ps.start_date<=@endDate		

 ) programa_tarv ON programa_tarv.patient_id=inicio_real.patient_id
) activos
GROUP BY patient_id 
   
   "

sql_query_openmrs_consulta_info <-  "

Select DATE_FORMAT(visitas.encounter_datetime  ,'%Y/%m/%d')  as data_visita , DATE_FORMAT(o.value_datetime  ,'%Y/%m/%d') 
as value_datetime, o.uuid
		from

			(	select 	e.patient_id, encounter_datetime
				from 	encounter e 
				where 	e.voided=0 and e.encounter_type in (9,6) 
                and patient_id =@patient_id  
			) visitas
			inner join encounter e on e.patient_id=visitas.patient_id
			inner join obs o on o.encounter_id=e.encounter_id			
			where o.concept_id=1410 and o.voided=0 and e.voided=0 and e.encounter_datetime=visitas.encounter_datetime and 
			e.encounter_type in (9,6)  and e.location_id=@location  group by data_visita  order  by  data_visita desc    
"



sql_query_openmrs_levant_info <-  "
Select DATE_FORMAT(visitas.encounter_datetime ,'%Y/%m/%d')  as pickup_date ,DATE_FORMAT(o.value_datetime, '%Y/%m/%d') as next_scheduled,
   o.uuid
		from

			(	select 	e.patient_id, encounter_datetime
				from 	encounter e 
				where 	e.voided=0 and e.encounter_type = 18
                and patient_id =@patient_id  
			) visitas
			inner join encounter e on e.patient_id=visitas.patient_id
			inner join obs o on o.encounter_id=e.encounter_id			
			where o.concept_id=5096 and o.voided=0 and e.voided=0 and e.encounter_datetime=visitas.encounter_datetime and 
			e.encounter_type =18  and e.location_id=@location group by pickup_date order  by  pickup_date desc
            
"


sql_query_openmrs_viral_load_info <- "


  SELECT 	e.patient_id,
				CASE o.value_coded
                WHEN 1306  THEN  'Nivel baixo de detencao'
                WHEN 23814 THEN  'Indectetavel'
                WHEN 23905 THEN  'Menor que 10 copias/ml'
                WHEN 23906 THEN  'Menor que 20 copias/ml'
                WHEN 23907 THEN  'Menor que 40 copias/ml'
                WHEN 23908 THEN  'Menor que 400 copias/ml'
                WHEN 23904 THEN  'Menor que 839 copias/ml'
                ELSE ''
                END  AS carga_viral_qualitativa,
				        DATE_FORMAT(ult_cv.data_cv_qualitativa  ,'%Y/%m/%d')  as data_ultima_carga
                DATE_FORMAT(o.value_numeric ,'%Y/%m/%d')  as valor_ultima_carga
                fr.name as Origem_Resultado
                FROM  encounter e 
                inner join	(
							SELECT 	e.patient_id,encounter_datetime as data_cv_qualitativa
							from encounter e inner join obs o on e.encounter_id=o.encounter_id
							where e.encounter_type IN (6,9,13,53) AND e.voided=0 AND o.voided=0 AND o.concept_id in( 856, 1305)  and 
							patient_id =@patient_id 
				) ult_cv 
                on e.patient_id=ult_cv.patient_id
				inner join obs o on o.encounter_id=e.encounter_id 
                 left join form fr on fr.form_id = e.form_id
                 where e.encounter_datetime=ult_cv.data_cv_qualitativa	
				and	e.voided=0  AND  e.location_id=@location AND   e.encounter_type in (6,9,13,53) and
				o.voided=0 AND 	o.concept_id in( 856, 1305) 
        and e.location_id=@location  group by encounter_datetime order  by  encounter_datetime desc
"



sql_query_mpi_drug_pickups <- "
SELECT drug_pickup.pickup_date,
    drug_pickup.next_scheduled,
    drug_pickup.patient_uuid,
    drug_pickup.location_uuid,
    drug_pickup.uuid
FROM ccs_mpi.drug_pickup
where patient_uuid = @patient_id ;
"
