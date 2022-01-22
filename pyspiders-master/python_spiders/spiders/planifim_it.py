# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import urllib.parse
import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class TheFisheyeViewSpider(Spider):
    name = 'planifim_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.planifim.it"]
    start_urls = ["https://www.planifim.it/index.php/component/realestatemanager/126/search?searchtext=&yearfrom=da&yearto=a&pricefrom=da&priceto=a&catid=46&listing_status=Tutto&property_type=Tutto&provider_class=Tutto&option=com_realestatemanager&task=search&Itemid=126"]

    def parse(self, response):
        for url in response.css("div.okno_R div.texthouse div.titlehouse a::attr(href)").getall():
            yield Request(url=url, callback=self.populate_item, dont_filter=True)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = "apartment"
        rent = response.css("div#currency_price div.pricemoney span.money::text").get().strip()
        currency = response.css("div#currency_price div.pricemoney span.price::text").get().strip()
        
        street_address = response.css("div.row_text:nth-child(7) > span:nth-child(2)::text").get().strip()
        city = response.css("div.row_text:nth-child(5) > span:nth-child(2)::text").get().strip()
        country = "Italia"
        address = f"{street_address} {city} {country}"
        
        title = response.css("div.row_text:nth-child(2) > span:nth-child(2)::text").get().strip()
        title_lowered = title.lower()
        if (
            ("commerciale" in title_lowered) 
            or ("ufficio" in title_lowered) 
            or ("magazzino" in title_lowered) 
            or ("box" in title_lowered) 
            or ("auto" in title_lowered) 
            or ("negozio" in title_lowered) 
            or ("vendita" in title_lowered) ):
            return
        
        images = response.css("div.table_gallery div.gallery_img div.thumbnail a::attr(href)").getall()
        images_to_add = []
        for image in images:
            img_src = self.allowed_domains[0] + urllib.parse.quote(image)
            images_to_add.append(img_src)

        room_count = response.css("div.row_text:nth-child(15) > span:nth-child(2)::text").get().strip()
        if(int(room_count) > 10):
            room_count = response.css("div.row_text:nth-child(13) > span:nth-child(2)::text").get().strip()
        
        description = response.css("p.MsoNormal span::text").getall()
        if(not description):
            description = response.css("p.Standard span::text").getall()
            description = " ".join(description)

        if(not description):
            description = response.css("span.col_02:nth-child(10) > p:nth-child(1)::text").get()

        if(not description):
            description = response.css("span.col_02:nth-child(8) > p:nth-child(1)::text").get()

        external_id = response.css("div.row_text:nth-child(1) > span:nth-child(2)::text").get()
        city = response.css("div.row_text:nth-child(5) > span:nth-child(2)::text").get()
        zipcode = response.css("div.row_text:nth-child(6) > span:nth-child(2)::text").get()
        
        utilities = response.css("div.row_text:nth-child(13) > span:nth-child(2)::text").get()
        utilities_found = re.search("[0-9]{2,}", utilities) 

        landlord_phone = "+39.080.5242627"
        landlord_email = "info@planifim.it"
        landlord_name = "planifim"

        bathroom_count = response.css("div.row_text:nth-child(14) > span:nth-child(2)::text").get()
        bathroom_count_found = re.search("[1-9]{1}", bathroom_count)
        if(not bathroom_count_found):
            bathroom_count = response.css("div.row_text:nth-child(17) > span:nth-child(2)::text").get()
        
        square_meters = response.css("div.row_text:nth-child(17) > span:nth-child(2)::text").get()
        square_meters_found = re.search("([0-9]{2,3})", square_meters)
        if(not square_meters_found):
            square_meters = response.css("div.row_text:nth-child(19) > span:nth-child(2)::text").get()
        square_meters_found = re.search("([0-9]{2,3})", square_meters)
        if(not square_meters_found):
            square_meters = response.css("div.row_text:nth-child(21) > span:nth-child(2)::text").get()

        property_features = response.css("div.row_text").getall()
        for feature in property_features:
            if("Piano:" in feature):
                floor = re.findall('<span class="col_text_2">(.+)</span>', feature)[0]

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("address", address)
        item_loader.add_value("title", title)
        item_loader.add_value("images", images_to_add)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("city", city)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("description", description)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("floor", floor)
        if(utilities_found):
            item_loader.add_value("utilities", utilities)
            
        yield item_loader.load_item()

