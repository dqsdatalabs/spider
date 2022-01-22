# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class BowesandcocksSpider(Spider):
    name = 'bowesandcocks_com'
    country='canada'
    locale='en' 
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    execution_type='testing'
    allowed_domains = ["www.bowesandcocks.com"]
    start_urls = ["https://bowesandcocks.com/search-mls-listings/?searchby=&search=&minbed=&minbath=&property_type_primary=1&property_type=&status_id=5&building_type=&last_only=&show_only=&year_build=&tags=&key_words=&form=&minprice=0&maxprice=&listing_number=6d24041142&board_ids=88%2C46%2C37%2C50%2C76%2C31%2C100%2C47"]


    def parse(self, response):
        site_pages = []
        pages = response.css("a.page-numbers::attr(href)").getall()
        for page in pages:
            site_page = int(re.findall("/page/([1-9]+)/", page)[0])
            site_pages.append(site_page)
        
        last_page_number = max(site_pages)
        for page_number in range(1, last_page_number + 1):
            url = f"https://bowesandcocks.com/search-mls-listings/page/{page_number}/?searchby&search&minbed&minbath&property_type_primary=1&property_type&status_id=5&building_type&last_only&show_only&year_build&tags&key_words&form&minprice=0&maxprice&listing_number=6d24041142&board_ids=88%2C46%2C37%2C50%2C76%2C31%2C100%2C47#038;search&minbed&minbath&property_type_primary=1&property_type&status_id=5&building_type&last_only&show_only&year_build&tags&key_words&form&minprice=0&maxprice&listing_number=6d24041142&board_ids=88%2C46%2C37%2C50%2C76%2C31%2C100%2C47"
            yield Request(response.urljoin(url), callback=self.populate_page, dont_filter = True)

    def populate_page(self, response):
        for page in response.css("div.property-detail div.info-btn a.btn-info::attr(href)").getall():
            yield Request(response.urljoin(page), callback=self.populate_item, dont_filter = True)
    
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = "apartment"
        rent = response.css("span.blueClr::text").get()
        currency = "CAD"
        if("/" in rent):
            rent = rent.split("/")[0]

        rent = rent.split(",")
        rent = "".join(rent)

        title = response.css("div.property_name h1::text").get()
        square_meters = response.css("ul.bluebullets li:contains('Floor Space:')::text").get()
        images = response.css("div.carousel-item a::attr(href)").getall()
        room_count = response.css("ul.bluebullets li:contains('Bedrooms:')::text").get()
        room_count = eval(room_count)
        bathroom_count = response.css(".col-3 > span:nth-child(1)::text").get().split(" ")[0]
        
        address = title 
        city = None
        zipcode = None
        try:
            city = address.split(",")[1]
            zipcode = address.split(",")[3]
        except:
            pass
        
        parking = response.css("ul.bluebullets li:contains('Total Parking Spaces:')::text").get()
        if(parking):
            parking = True
        else: 
            parking = False

        description = response.css(".yoa_property_detail_full > div:nth-child(1) > div:nth-child(1) > p:nth-child(1)::text").get()
        furnished = None
        if("furnished" in description):
            furnished = True
        landlord_name = response.css("div.profile-detail div.name-info strong::text").get()
        contact_info = response.css("div.contact-info ul li span a::text").getall()
        landlord_phone = None
        landlord_email = None
        external_id = None
        try:
            landlord_phone = contact_info[0]
            landlord_email = contact_info[-1]
            external_id = response.css("div.property_code strong.title::text").get().split("ID#:")[1]
        except:
            pass
        
        latitude_and_longitude = response.css("li#menu-item-500  a::attr(href)").get().split("=")[1].split(",")
        latitude = latitude_and_longitude[0]
        longitude = latitude_and_longitude[1].split("&")[0]

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("title", title)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("square_meters", int(int(square_meters)*10.764))
        item_loader.add_value("images", images)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("parking", parking)
        item_loader.add_value("description", description)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("furnished", furnished)

        yield item_loader.load_item()
