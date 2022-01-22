# -*- coding: utf-8 -*-
# Author: Mohamed Zakaria

import re

from scrapy import Spider, Request
from python_spiders.loaders import ListingLoader

class StudiopontevecchioSpider(Spider):
    name = 'studiopontevecchio_it'
    country='italy'
    locale='it' 
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type='testing'
    allowed_domains = ["www.studiopontevecchio.it"]
    start_urls = ["https://www.studiopontevecchio.it/it/immobili.php?q=affitto"]

    def parse(self, response):
        for url in response.css("a.button1::attr(href)").getall():
            yield Request( response.urljoin(url), callback=self.populate_item )

    def populate_item(self, response):
        data = {}

        data["property_type"] = "apartment"
        data["external_link"] = response.url
        data["title"] = response.css(".banner-text_a > h2:nth-child(1)::text").get()
        
        data["landlord_name"] = "Agenzia immobiliare a Firenze"
        data["landlord_phone"] = "055.2335502"
        data["landlord_email"] = "info@studiopontevecchio.it"

        data["city"] = "Firenze"
        data["external_id"] = response.css(".indent3 > h3:nth-child(1)::text").getall()[1].split("Rif.")[1]
        data["rent"] = response.css(".ric_rif::text").get().split(":")[1]
        data["description"] = response.css(".ric_descr > span:nth-child(2)::text").get()
                
        table_rows = response.css("div.ric_tab_dett table tr td").getall()
        data["energy_label"] = None
        data["square_meters"] = None
        data["room_count"] = None
        for row in table_rows:
            if("Superficie Mq:" in row):
                data["square_meters"] = re.findall("(\d+)", row)[1]
            
            if("Numero vani:" in row):
                data["room_count"] = re.findall("(\d+)", row)[1]
            
            if("Classe Energetica:" in row):
                try:
                    data["energy_label"] = re.findall("<strong> ([A-Z])</strong>", row)[0]
                except:
                    pass
            
        image_url = "https://www.studiopontevecchio.it/it/include/rif_gallery.php?" + response.url.split("?")[1]
        yield Request( response.urljoin(image_url), callback=self.get_images, meta = {"data": data} )


    def get_images(self, response):
        item_loader = ListingLoader(response=response)
        data = response.meta.get("data")

        images = response.css("image::attr(imageURL)").getall()

        item_loader.add_value("external_link", data["external_link"])
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", data["property_type"])
        item_loader.add_value("title", data["title"])
        item_loader.add_value("landlord_name", data["landlord_name"])
        item_loader.add_value("landlord_phone", data["landlord_phone"])
        item_loader.add_value("landlord_email", data["landlord_email"])
        item_loader.add_value("city", data["city"])
        item_loader.add_value("external_id", data["external_id"])
        item_loader.add_value("rent_string", data["rent"])
        item_loader.add_value("description", data["description"])
        item_loader.add_value("energy_label", data["energy_label"])
        item_loader.add_value("square_meters", data["square_meters"])
        item_loader.add_value("room_count", data["room_count"])
        item_loader.add_value("images", images)
       
        yield item_loader.load_item()
