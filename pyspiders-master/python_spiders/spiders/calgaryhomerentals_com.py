# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class QuattrovaniSpider(Spider):
    name = 'calgaryhomerentals_com'
    country='canada'
    locale='en' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.calgaryhomerentals.com"]
    start_urls = ["https://calgaryhomerentals.com/advanced-search/?adv_location=&filter_search_action%5B%5D=for-rent&submit=SEARCH&is10=10&filter_search_action%5B%5D=&advanced_city=&advanced_area=&bedrooms=&baths=&pets=&available=&price_low=500&price_max=8000&wpestate_regular_search_nonce=b5d34536f9&_wp_http_referer=%2F"]

    def parse(self, response):
        for url in response.css("h4 a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = "house"

        title = response.css("h1.entry-title::text").get()
        rent = response.css("span.price_area::text").get()
        rent = re.findall("[0-9]+", rent)
        rent = "".join(rent)
        currency = "CAD"

        address = response.css("span.adres_area::text").get()
        rest_of_address = response.css("span.adres_area a::text").getall()
        city = response.css("strong:contains('City:') + a::text").get()

        listings_details = response.css("div.listing_detail::text").getall()
        zipcode = listings_details[4]
        
        rest_of_address = " ".join(rest_of_address)
        address = f"{address} {rest_of_address}"

        images = response.css("img.img-responsive::attr(src)").getall()
        description = response.css("div.wpestate_property_description p::text").getall()
        description = " ".join(description)
        external_id = listings_details[6]
        
        square_meters = listings_details[8]
        square_meters = re.findall("[0-9]+",square_meters)
        square_meters = "".join(square_meters)
        square_meters = int(int(float(square_meters))/10.763)

        room_count = listings_details[10]
        bathroom_count = listings_details[11]
        bathroom_count = int(float(bathroom_count))
        
        pets_allowed = listings_details[13]
        if("no" in pets_allowed.lower()):
            pets_allowed = False
        else:
            pets_allowed = True

        available_date = listings_details[14]
        
        listings_details = " ".join(listings_details)
        dishwasher = "dishwasher" in listings_details
        parking = "garage" in listings_details
        washing_machine = "washer and dryer" in listings_details

        landlord_name = "calgaryhomerentals"
        landlord_phone = "403-258-3944"
        landlord_email = "info@calgaryhomerentals.com"

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("title", title)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("images", images)
        item_loader.add_value("description", description)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("square_meters", int(int(square_meters)*10.764))
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("pets_allowed", pets_allowed)
        item_loader.add_value("available_date", available_date)
        item_loader.add_value("parking", parking)
        item_loader.add_value("dishwasher", dishwasher)
        item_loader.add_value("parking", parking)
        item_loader.add_value("washing_machine", washing_machine)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
       
        yield item_loader.load_item()
