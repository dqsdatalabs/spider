# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class StudioPianettaSpider(Spider):
    name = 'studiopianetta_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.https://studiopianetta.it"]
    start_urls = ["https://studiopianetta.it/immobili.asp?av=1"]

    def parse(self, response):
        a_tags = response.css("div.mbr-table-md-up div a.btn::attr(href)").getall()
        properties_urls = []
        
        for a in a_tags:
            properties_urls.append(self.allowed_domains[0] +"/"+ a )
        
        for url in a_tags:

            yield Request(response.urljoin(url), callback=self.populate_item, dont_filter=True)

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = "apartment"
        rent = response.css(".titolino > span:nth-child(4) > b:nth-child(1)::text").get()
        title = response.css("h3.mbr-section-title span::text").get()
        room_count = response.css("div.col-md-4:nth-child(1) > b:nth-child(1)::text").get()
        bathroom_count = response.css("div.col-md-4:nth-child(2) > b:nth-child(1)::text").get()
        
        energy_label = response.css("div.col-md-4:nth-child(3) > b:nth-child(1)::text").get()
        if(energy_label != "Non indicata"):
            energy_label = energy_label.split(" - ")[1]

        images = response.css("div.mbr-gallery-item div img::attr(style)").getall()
        images_src = []
        for image in images:
            src = image.split("background-image:url('")[1]
            src = src.split("'); background-repeat:no-repeat; ")[0]
            images_src.append(src)

        images_to_add = []
        for src in images_src: 
            images_to_add.append(self.allowed_domains[0] + '/' + src)

        landlord_name = "studiopianetta"
        landlord_email = "info@studiopianetta.it - studiopianetta@pec.it"
        landlord_phone = "+39 055 2479892 - +39 335 8299902"

        city = "Florence"
        description = response.css("#content1-j > div:nth-child(1) > div:nth-child(1) > div:nth-child(1)::text").get().strip()
        external_id = response.url.split("det=")[1]

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("rent_string", rent)
        item_loader.add_value("title", title)
        item_loader.add_value("room_count", room_count)
        item_loader.add_value("bathroom_count", bathroom_count)
        item_loader.add_value("energy_label", energy_label)
        item_loader.add_value("images", images_to_add)
        item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("city", city)
        item_loader.add_value("description", description)
        item_loader.add_value("external_id", external_id)
       
        yield item_loader.load_item()

