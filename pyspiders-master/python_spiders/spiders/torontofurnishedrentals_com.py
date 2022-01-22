# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re
import json

from scrapy import Spider, Request, FormRequest
from python_spiders.loaders import ListingLoader
from python_spiders.helper import extract_location_from_address, extract_location_from_coordinates, remove_white_spaces

class Torontofurnishedrentals_comSpider(Spider):
    name = 'torontofurnishedrentals_com'
    country='canada'
    locale='en' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.torontofurnishedrentals.com"]
    # start_urls = ["https://www.torontofurnishedrentals.com/wp-admin/admin-ajax.php?id=&post_id=18&slug=furnished-apartments&canonical_url=https%3A%2F%2Fwww.torontofurnishedrentals.com%2Ffurnished-apartments%2F&posts_per_page=100&page=1&offset=0&post_type=rentals&repeater=template_3&seo_start_page=1&preloaded=false&preloaded_amount=0&order=DESC&orderby=post__in&post__in=586,1757,7681,8519,8061,1399,1424,1323,8870,2750,8885,7695,4159,8326,8554,8540,6613,7783,7727,6858,6881,6176,1923,1970,5659,7590,6344,9038,8304,8695,8342,8462,1308,7270,7249,7205,1040,7156,1134,7113,7049,1199,6926,1225,4195,7333,6483,1654,6158,1932,1953,5478,5435,3799,3825,1968,5005,4176,4534,7288,1176,7372,7403,561,7531,490,8242,7839,6741,8496,8590,7352,6320,7873,1729,7855,8854,5241,5276,7902,6787,7932,7919,7662,418,447,8940,1017,8075,7569,8443,8158,8427,4494,7946,1253,6375,6976,6951,5172,8482,8139,7974,8196,3067,8900,8641,8411,7508,8608,2310,2611,2447,2668,2633,2828,399,3178,512,2127,6904,7083,5207,5042,7710,7887,4549,4934,2814,2782,2708,2690,2500,848,372,2051,1290,6223,2361,2588,2391,2852,334,3038,4373,4351,2912,3928,3773,1305,2416,6831,7743,1106,4404,2520,7757,2571,7133,1072,2893,8914,8571,7483,2924,7441,2951,2981,3093,781,3157,716,2795,7551,644,611,8284,4444,6807,3197,2164,2224,2258,3191,3195,2336,2542,3202,5592,3218,1752,4122,4515,4216,3211,3209,2013,2104,5629,8015,1448,1626,6286,3182,3170,3172,3174,6403,3180,6434,1573,3205,9015,8976,2195,1499,5356,1547,4978,1600,7461,6546,1525,936,467,7312,2281,1824,5524,1723,7181,1156,993,967,881,1901,3216,3186,3125,3214,3199,3124,3193,3126,3127,3188,3141,3184,3137,3139,3165,3143,156,3168,226,3147,3149,2649,3151,2727,3153,3207,1471,4248,6024,1778,5078,6133,167,1852,671,7001,7816,1877,743,1848,812,3129,3120,3145,3123,3128,3222,3231,3228,3233,3235,3238,3240,3242,3246,3176,3249,3251,3253,3225,3163,4290,3161,3155,3159,3244,3221,916&action=alm_get_posts&query_type=standard"]
    position = 1
    
    def start_requests(self):
        yield Request(url=f"https://www.torontofurnishedrentals.com/wp-admin/admin-ajax.php?id=&post_id=18&slug=furnished-apartments&canonical_url=https%3A%2F%2Fwww.torontofurnishedrentals.com%2Ffurnished-apartments%2F&posts_per_page=100&page=1&offset=0&post_type=rentals&repeater=template_3&seo_start_page=1&preloaded=false&preloaded_amount=0&order=DESC&orderby=post__in&post__in=586,1757,7681,8519,8061,1399,1424,1323,8870,2750,8885,7695,4159,8326,8554,8540,6613,7783,7727,6858,6881,6176,1923,1970,5659,7590,6344,9038,8304,8695,8342,8462,1308,7270,7249,7205,1040,7156,1134,7113,7049,1199,6926,1225,4195,7333,6483,1654,6158,1932,1953,5478,5435,3799,3825,1968,5005,4176,4534,7288,1176,7372,7403,561,7531,490,8242,7839,6741,8496,8590,7352,6320,7873,1729,7855,8854,5241,5276,7902,6787,7932,7919,7662,418,447,8940,1017,8075,7569,8443,8158,8427,4494,7946,1253,6375,6976,6951,5172,8482,8139,7974,8196,3067,8900,8641,8411,7508,8608,2310,2611,2447,2668,2633,2828,399,3178,512,2127,6904,7083,5207,5042,7710,7887,4549,4934,2814,2782,2708,2690,2500,848,372,2051,1290,6223,2361,2588,2391,2852,334,3038,4373,4351,2912,3928,3773,1305,2416,6831,7743,1106,4404,2520,7757,2571,7133,1072,2893,8914,8571,7483,2924,7441,2951,2981,3093,781,3157,716,2795,7551,644,611,8284,4444,6807,3197,2164,2224,2258,3191,3195,2336,2542,3202,5592,3218,1752,4122,4515,4216,3211,3209,2013,2104,5629,8015,1448,1626,6286,3182,3170,3172,3174,6403,3180,6434,1573,3205,9015,8976,2195,1499,5356,1547,4978,1600,7461,6546,1525,936,467,7312,2281,1824,5524,1723,7181,1156,993,967,881,1901,3216,3186,3125,3214,3199,3124,3193,3126,3127,3188,3141,3184,3137,3139,3165,3143,156,3168,226,3147,3149,2649,3151,2727,3153,3207,1471,4248,6024,1778,5078,6133,167,1852,671,7001,7816,1877,743,1848,812,3129,3120,3145,3123,3128,3222,3231,3228,3233,3235,3238,3240,3242,3246,3176,3249,3251,3253,3225,3163,4290,3161,3155,3159,3244,3221,916&action=alm_get_posts&query_type=standard",
                    callback=self.parse,
                    method='GET', dont_filter = True)

    

    def parse(self, response):
        parsed_response_properties = json.loads(response.body)

        for url in re.findall('<a href="(.+)">', parsed_response_properties["html"]):
            yield Request(response.urljoin(url), callback = self.populate_item)

    def populate_item(self, response):
        
        property_type = "apartment"

        title = response.css("h1.title::text").get()
        rent = response.css("span.al-price::text").get()
        rent = re.findall("([0-9]+)", rent)
        rent = "".join(rent)
        currency = "EUR"

        deposit = response.css("span.al-deposit::text").get()
        deposit = re.findall("([0-9]{2,})", deposit)
        if(len(deposit) > 1):        
            deposit = int(deposit[0]) + int(deposit[1])
        else:
            deposit = None

        room_count = response.css("span.al-nrb::text").get()
        room_count = re.findall("([0-9])", room_count)
        if( len(room_count) > 0):
            room_count = room_count[0]
        else:
            room_count = "1"

        bathroom_count = response.css("span.al-nrbt::text").get()
        bathroom_count = re.findall("([0-9])", bathroom_count)
        if( len(bathroom_count) > 0):
            bathroom_count = bathroom_count[0]
        else:
            bathroom_count = None

        
        square_meters = response.css("span.al-sqrfr::text").get()
        square_meters = re.findall("([0-9]+)", square_meters)
        if( len(square_meters) > 0):
            square_meters = square_meters[0]
        else:
            square_meters = None

        description = response.css("div#dslc-theme-content-inner p::text").getall()
        description = " ".join(description)

        features = response.css("div.al-details span::text").getall()
        features = " ".join(features).lower()

        swimming_pool = "swimming pool" in features
        washing_machine = "washer/dryer" in features
        parking = "parking" in features
        furnished = "furnished" in features

        images = response.css("noscript::attr(data-data-pin-media)").getall()

        location_script = response.css("script:contains('initMap()')::text").get()
        latitude = re.findall("lat: (-?[0-9]+\.[0-9]+)", location_script)[0]
        longitude = re.findall("lng: (-?[0-9]+\.[0-9]+)", location_script)[0]

        location_data = extract_location_from_coordinates(longitude, latitude)
        zipcode = location_data[0]
        city = location_data[1]
        address = location_data[2]

        landlord_name = "torontofurnishedrentals"
        landlord_phone = "1-888-787-7829"
        landlord_email = "reservations@torontofurnishedrentals.com"

        item_loader = ListingLoader(response=response)
        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        # item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        self.position += 1
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        # item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        item_loader.add_value("bathroom_count", bathroom_count) # Int

        # # item_loader.add_value("available_date", available_date) # String => date_format

        # # item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        item_loader.add_value("furnished", furnished) # Boolean
        item_loader.add_value("parking", parking) # Boolean
        # # item_loader.add_value("elevator", elevator) # Boolean
        # item_loader.add_value("balcony", balcony) # Boolean
        # # #item_loader.add_value("terrace", terrace) # Boolean
        item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        item_loader.add_value("washing_machine", washing_machine) # Boolean
        # # item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        # # # item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # # Monetary Status
        item_loader.add_value("rent_string", rent) # Int
        item_loader.add_value("deposit", deposit) # Int
        # # #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        # # #item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", currency) # String

        # # #item_loader.add_value("water_cost", water_cost) # Int
        # item_loader.add_value("heating_cost", heating_cost) # Int

        # # #item_loader.add_value("energy_label", energy_label) # String

        # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_phone) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        yield item_loader.load_item()
