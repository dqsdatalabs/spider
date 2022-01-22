# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class ImmobiliareCostaSpider(Spider):
    name = 'immobiliarecosta_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.immobiliarecosta.it"]
    start_urls = ["https://www.immobiliarecosta.it/property-status/affitto/"]

    def parse(self, response):
        for url in response.css("div.title a::attr(href)").getall():
            yield Request( response.urljoin(url), callback=self.populate_item)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        title = response.css("h1.page-title::text").get()
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

        property_type = "apartment"
        
        rent = response.css("div.price span::text").get()
        currency = "EUR"

        landlord_name = "immobiliarecosta"
        landlord_phone = "+39 3272562149"
        landlord_email = "info@immobiliarecosta.it"

        external_id = response.css(".meta-box-list > li:nth-child(1)::text").get()
        room_count = response.css(".meta-box-list > li:nth-child(2)::text").get()
        bathroom_count = response.css(".meta-box-list > li:nth-child(3)::text").get()
        if(not bathroom_count):
            bathroom_count = room_count

        description = response.css(".large-8 > div:nth-child(1) > p:nth-child(5)::text").get()
        images = response.css("div.item a img::attr(src)").getall()
        latitude = response.css("div.map-wrap::attr(data-latitude)").get()
        longitude = response.css("div.map-wrap::attr(data-longitude)").get()
        
        balcony = None
        featurs_data = response.css("ul.large-block-grid-3 li").getall()
        for feature in featurs_data:
            if ("Balconato" in feature):
                has_balcony = re.search('<li class="active">', feature)
                if(has_balcony):
                    balcony = True
                else:
                    balcony = False

        city = response.css(".sub-title > ul:nth-child(1) > li:nth-child(1) > a:nth-child(1)::text").get()
        square_meters = None
        try:
            square_meters = re.findall("([0-9]{2,}) mq", description)[0]
        except IndexError:
            pass

        address = city

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("title", title)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("external_id", external_id)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("description", description)
        item_loader.add_value("images", images)
        item_loader.add_value("latitude", latitude)
        item_loader.add_value("longitude", longitude)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("city", city)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("address", address)
       
        yield item_loader.load_item()
