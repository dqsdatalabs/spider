# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class WbimmobiliareSpider(Spider):
    name = 'Wbimmobiliare_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.wbimmobiliare.it"]
    start_urls = ["https://wbimmobiliare.it/ricerca-avanzata/?status=in-affitto"]

    def parse(self, response):
        for url in response.css("h2.property-title a::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter = True)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = response.css("ul.ere-property-list:nth-child(1) > li:nth-child(3) > span:nth-child(2)::text").get()
        if( property_type != "Appartamento"):
            return

        property_type = "apartment"
        rent = response.css("span.property-price::text").get().strip()
        title = response.css("div.property-heading h2::text").get().strip()
        
        property_info = response.css("p.property-info-value::text").getall()
        square_meters = property_info[1].strip()
        room_count = property_info[3].strip()
        bathroom_count = property_info[4].strip()

        images = response.css("div.property-gallery-item img::attr(src)").getall()
        
        country = response.css("#property-822 > div.single-property-element.property-location > div.ere-property-element > ul > li:nth-child(1) > span::text").get()
        city = response.css("#property-822 > div.single-property-element.property-location > div.ere-property-element > ul > li:nth-child(2) > span::text").get()
        address = f"{country} {city}"

        landlord_name = response.css("div.agent-heading h4 a::text").get()
        landlord_email = response.css("div.agent-mobile span::text").get()
        landlord_phone = response.css("div.agent-email span::text").get()

        description = response.css("#tab-panel-1 > p:nth-child(1)::text").get()
        external_id = response.css(".property-id > div:nth-child(2) > p:nth-child(1)::text").get()

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("title", title)
        item_loader.add_value("square_meters", square_meters)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("images", images)
        item_loader.add_value("description", description)
        item_loader.add_value("external_id", external_id)
       
        yield item_loader.load_item()



