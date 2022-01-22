# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class McdadiSpider(Spider):
    name = 'mcdadi_com'
    country='canada'
    locale='en' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.mcdadi.com"]
    start_urls = ["https://mcdadi.com/"]
    count = 0

    def parse(self, response):
        url = response.css("li#menu-item-4523 a::attr(href)").get()
        yield Request(url = url, method='GET', headers = {"Accept": "*/*", "Accept-Encoding": "gzip, deflate, br"}, callback=self.get_rentals_page, dont_filter = True)

    def return_page_url(self, number):
        return f"http://listings.mcdadi.com/communities/rentals/page/{number}/"

    def get_rentals_page(self, response):
        site_pages = response.css("ul.pagination li a::attr(href)").getall()
        last_page_url = site_pages[-1]
        last_page_number = re.findall("page/([0-9]+)/", last_page_url)[0]
        for number in range(1, int(last_page_number) + 1):
            url = self.return_page_url(number)
            yield Request(response.urljoin(url), headers = {"Accept": "*/*", "Accept-Encoding": "gzip, deflate, br"}, callback=self.populate_page, dont_filter = True)
            


    def populate_page(self, response):
        for page in response.css("a.details::attr(href)").getall():
            yield Request(response.urljoin(page), headers = {"Accept": "*/*", "Accept-Encoding": "gzip, deflate, br"}, callback=self.populate_item, dont_filter = True)
    
    
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = "apartment"
        title = response.css("h1.location::text").get()
        
        address = title
        city = address.split(",")[-1]
   
        property_details = response.css("h2.details::text").get()
        property_details = property_details.split("/")
        rent = property_details[2]
        rent = rent.split(",")
        rent = "".join(rent)
        currency = "CAD"
        room_count = re.findall("([0-9]+) Bed", property_details[0])[0]
        bathroom_count = re.findall("([0-9]+) Bath", property_details[1])[0]
        external_id = property_details[3].split(":")[1]

        description = response.css("div.content p::text").getall()
        description = " ".join(description)
        description = re.sub("Listing Office: SAM MCDADI REAL ESTATE INC., BROKERAGE Loading WalkScore data... Loading Mortgage Calculators...", "", description)

        images = response.css("ul#mainListingGalery li a img::attr(src)").getall()

        swimming_pool = response.css("th:contains('Pool') + th::text").get()
        if("None" in swimming_pool):
            swimming_pool = False
        else:
            swimming_pool = True

        parking = response.css("th:contains('Garages') + th::text").get()
        if(int(eval(parking))):
            parking = True
        else:
            parking = False


        landlord_name = "mcdadi"
        landlord_phone = "(416) 801-2400"
        landlord_email = "sam@mcdadi.com"

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("title", title)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("images", images)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("parking", parking)
        item_loader.add_value("swimming_pool", swimming_pool)
        item_loader.add_value("description", description)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("external_id", external_id)
       
        yield item_loader.load_item()
