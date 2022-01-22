# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class CochibaImmobiliareSpider(Spider):
    name = 'Cochibaimmobiliare_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.cochibaimmobiliare.it"]
    start_urls = ["https://www.cochibaimmobiliare.it/appartamenti-a-torino-affitto/"]

    def parse(self, response):
        for url in response.css("div.es-details-flex span.es-read-wrap a.es-button::attr(href)").getall():
            yield Request(response.urljoin(url), callback=self.populate_item)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = "apartment"
        rent = response.css(".es-price__wrap > span:nth-child(1) > span:nth-child(1)::text").get()
        title = response.css("h1.page-title::text").get()
        room_count = response.css(".es-property-fields > ul:nth-child(2) > li:nth-child(1)::text").get().strip()
        bathroom_count = response.css(".es-property-fields > ul:nth-child(2) > li:nth-child(2)::text").get()
        images = response.css("img.attachment-thumbnail::attr(src)").getall()

        description = response.css("div#es-description p::text").getall()
        description = " ".join(description)

        furnished = None
        furnished_data = response.css("div.es-features-list-wrap:nth-child(2) > ul:nth-child(2) li::text").getall()
        for element in furnished_data:
            if "Arredato" in element:
                if "Non" in element:
                    furnished = False
                else:
                    furnished = True

        landlord_name = "COCHIBA IMMOBILIARE-AGENZIA IMMOBILIARE TORINO CENTRO"
        landlord_phone = "011.580.81.51"
        landlord_email = "info@cochibaimmobiliare.it"

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("title", title)
        item_loader.add_value("room_count", int(float(room_count)))
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("images", images)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("description", description)
    
        if furnished != None:
            item_loader.add_value("furnished", furnished)
       
        yield item_loader.load_item()
