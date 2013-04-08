@use_write_api_client
Feature: the performance platform write api

    Scenario: hitting the health check url
         When I go to "/_status"
         then I should get back a status of "200"

    Scenario: posting to the health check URL
        Given I have the data in "dinosaur.json"
         when I post the data to "/_status"
         # Bah, this should be a 405
         then I should get back a status of "400"

    Scenario: posting to a reserved bucket name
        Given I have the data in "dinosaur.json"
         when I post the data to "/_bucket"
         then I should get back a status of "400"

    Scenario: posting one object to a bucket
        Given I have the data in "dinosaur.json"
         when I post the data to "/my_dinosaur_bucket"
         then I should get back a status of "200"
         and  the stored data should contain "1" "name" equaling "t-rex"

    Scenario: posting a list of objects to a bucket
        Given I have the data in "dinosaurs.json"
         when I post the data to "/my_dinosaur_bucket"
         then I should get back a status of "200"
         and  the stored data should contain "2" "size" equaling "big"
         and  the stored data should contain "1" "name" equaling "microraptor"

    Scenario: tagging data with week start at
        Given I have the data in "timestamps.json"
         when I post the data to "/data-with-times"
         then I should get back a status of "200"
          and the stored data should contain "3" "_week_start_at" on "2013-03-11"
          and the stored data should contain "2" "_week_start_at" on "2013-03-18"
