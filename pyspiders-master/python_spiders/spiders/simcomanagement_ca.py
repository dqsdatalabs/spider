# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class SimcomanagementSpider(Spider):
    name = 'simcomanagement_ca'
    country='canada'
    locale='en' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.simcomanagement.ca"]
    start_urls = ["https://simcomanagement.ca/rentals-division/?limit=100&wplpage=1"]

    def parse(self, response):
        site_pages = {}
        for url in response.css("a.view_detail::attr(href)").getall():
            site_pages[url] = url
        for page_key in site_pages:
            yield Request(url = site_pages[page_key], callback=self.populate_item, dont_filter = True)
            
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = response.css("div:contains('Property Type : ') > span::text").get()
        property_type = property_type.lower()
        if("house" in property_type):
            property_type = "house"
        else:
            property_type = "apartment"
        title = response.css("h1.title_text::text").get()
        rent = response.css("div.price_box::text").get()
        rent = rent.split(",")
        rent = "".join(rent)
        rent = re.findall("([0-9]+)", rent)[0]
        rent = int(rent)
        currency = "CAD"

        external_id = response.css("ul li.wpl-listing-id span.value::text").get()
        room_count = response.css("ul li.wpl-bedroom span.value::text").get()
        bathroom_count = response.css("ul li.wpl-bathroom span.value::text").get()
        zipcode = response.css("div#wpl-dbst-show41-PostalCode span::text").get()
        city = response.css("div#wpl-dbst-show41-City span::text").get()
        address = response.css("span.wpl-location::text").get()
        
        appliances_data = response.css("div.feature::text").getall()
        parking = "Parking" in appliances_data
        dishwasher = "Dishwasher" in appliances_data
        washing_machine = "Washing Machine" in appliances_data
        balcony = "Balcony" in appliances_data
        elevator = "Elevator" in appliances_data

        images = response.css("img.wpl_gallery_image::attr(src)").getall()


        landlord_name = "Simco Management (Calgary) Inc."
        landlord_phone = "(403) 234-0166"
        landlord_email = "rental@simcomgt.com"

        description = response.css("div.wpl_prp_show_detail_boxes_cont p::text").getall()
        description = " ".join(description[0:-3])

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("title", title)
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("description", description)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("city", city)
        item_loader.add_value("images", images)
        item_loader.add_value("parking", parking)
        item_loader.add_value("dishwasher", dishwasher)
        item_loader.add_value("washing_machine", washing_machine)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("address", address)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
       
        yield item_loader.load_item()
