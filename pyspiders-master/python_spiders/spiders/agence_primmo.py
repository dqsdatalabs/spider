# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'agence_primmo'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source = "AgencePrimmo_PySpider_france"

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://agence-primmo.com/location/?type_bien%5B%5D=appartement&localisation=&city=",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://agence-primmo.com/location/?type_bien%5B%5D=maison-villa&localisation=&city="
                ],
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='iwp__item-content']//h5/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)


        room = response.xpath("//span[contains(text(),'pièces')]/text()").get()
        if room:
            room = room.split()[0]
            item_loader.add_value("room_count",room)

        # square_meters = response.xpath("//strong[contains(.,'Surface')]/following-sibling::span/text()").get()
        # if square_meters:
        #     square_meters = square_meters.split()[0]
        #     item_loader.add_value("square_meters",square_meters)

        floor = response.xpath("//strong[contains(.,'Nombre')]/following-sibling::span/text()").get()
        if floor:
            item_loader.add_value("floor",floor)

        rent = response.xpath("//span[@class='price']").get()
        if rent:
            rent = rent.strip("€")
            item_loader.add_value("rent",rent)

        description = response.xpath("//div[@class='description']/text()").get()
        if description:
            item_loader.add_value("description",description)

        title = response.xpath("//div[@class='iwp__header-address']/h1/text()").get()
        if title:
            item_loader.add_value("title",title)

        external_id = response.xpath("//span[contains(text(),'Ref')]/text()").get()
        if external_id:
            external_id = external_id.split()[-1]
            item_loader.add_value("external_id",external_id)

        energy_label = response.xpath("//img[contains(@src,'etiquette')]/@src").get()
        if energy_label:
            energy_label = energy_label[-5]    
            energy_label:str
            if energy_label.isupper():
                item_loader.add_value("energy_label",energy_label)

        images = response.xpath("//div[@id='slider-for']/a/@href").getall()
        if images:
            item_loader.add_value("images",images)
            item_loader.add_value("external_images_count",len(images))

        terrace = response.xpath("//strong[contains(text(),'Terrasse')]").get()
        if terrace:
            item_loader.add_value("terrace",True)

        bathroom_count = response.xpath("//img[@alt='Salle de bain']/../../span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)

        parking = response.xpath("//strong[contains(text(),'Parking')]").get()
        if parking:
            item_loader.add_value("parking",True)

        utilities = response.xpath("//strong[contains(text(),'Honoraires')]/../following-sibling::div/span/text()").get()
        if utilities:
            utilities = utilities.strip("€")
            item_loader.add_value("utilities",utilities)

        deposit = response.xpath("//strong[contains(text(),'Dépot')]/../following-sibling::div/span/text()").get()
        if deposit:
            deposit = deposit.strip("€")
            item_loader.add_value("deposit",deposit)

        square_meters = response.xpath("//img[@alt='Surface']/../../span/text()").get()
        if square_meters:
            square_meters = square_meters.split(".")[0]
            item_loader.add_value("square_meters",square_meters)

        if "meublé" in title:
            item_loader.add_value("furnished",True)

        item_loader.add_value("address","Saint-Cyr-au-Mont-d'Or")
        item_loader.add_value("city","Lyon")
        item_loader.add_value("currency","EUR")
        item_loader.add_value("landlord_name","PRIMMO agencies")
        item_loader.add_value("landlord_email","gestion@agence-primmo.com")
        item_loader.add_value("landlord_phone","+33 4 78 47 43 91")



        yield item_loader.load_item()