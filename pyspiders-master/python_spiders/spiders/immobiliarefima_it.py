# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re


class MySpider(Spider):
    name = 'immobiliarefima_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Immobiliarefima_PySpider_italy"

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.immobiliarefima.it/ricerca.php?IDCategoria=2&CodiceImmobile=&IDProvincia=&IDComune=&IDTipologia=1&VaniA=&PrezzoDa=0&PrezzoA=2500000",
                    "https://www.immobiliarefima.it/ricerca.php?IDCategoria=2&CodiceImmobile=&IDProvincia=&IDComune=&IDTipologia=21&VaniA=&PrezzoDa=0&PrezzoA=2500000",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.immobiliarefima.it/ricerca.php?IDCategoria=2&CodiceImmobile=&IDProvincia=&IDComune=&IDTipologia=2&VaniA=&PrezzoDa=0&PrezzoA=2500000",
                    "https://www.immobiliarefima.it/ricerca.php?IDCategoria=2&CodiceImmobile=&IDProvincia=&IDComune=&IDTipologia=15&VaniA=&PrezzoDa=0&PrezzoA=2500000",
                    "https://www.immobiliarefima.it/ricerca.php?IDCategoria=2&CodiceImmobile=&IDProvincia=&IDComune=&IDTipologia=3&VaniA=&PrezzoDa=0&PrezzoA=2500000"
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
        
        for item in response.xpath("//h1/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})




    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        external_id = (response.url).split("/")[-1].split("-")[0]
        item_loader.add_value("external_id",external_id)
        title = response.xpath("//div[@class='heading-properties-3']/h1/text()").get()
        item_loader.add_value("title",title)

        zipcode = response.xpath("//div[@class='heading-properties-3']/span/strong/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode)

        address = response.xpath("//span[@class='location']/text()").get()
        if address:
            item_loader.add_value("address", address)
        rent = response.xpath("//span[@class='property-price']/text()").getall()[-1].strip()
        if rent:
            try:
                rent = rent.replace(".","")
                rent=int(rent)
                item_loader.add_value("rent",rent)
            except:
                return

        square_meters = response.xpath("//li[contains(text(),'Mq')]/text()").get().strip(" Mq.")
        if square_meters:
            item_loader.add_value("square_meters",square_meters)

            
        energy_label = response.xpath("//img[contains(@title,'Classe')]/@title").get()
        if energy_label:
            energy_label = energy_label.split()[-1]
            item_loader.add_value("energy_label",energy_label)
        
        room_count = response.xpath("//li[contains(text(),'Mq')]/text()").getall()[1].strip(", ").split(",")[0].split()[0]
        if room_count:
            item_loader.add_value("room_count",room_count)   

        balcony = response.xpath("//li[contains(text(),'Balconi')]").get()
        if balcony:
            item_loader.add_value("balcony",True)
             
        parking = response.xpath("//li[contains(text(),'Garage')]").get()
        if parking:
            item_loader.add_value("parking",True)

        description = response.xpath("//div[@class='properties-description mb-40']/p/text()").getall()
        if description:
            result = ""
            for desc in description:
                result = result + desc
            item_loader.add_value("description",result)

            if "arredato" in result.lower():
                item_loader.add_value("furnished",True)



        images = response.xpath("//img[@class='img-fluid']/@src").getall()
        if images:
            external_images_count = len(images)
            item_loader.add_value("external_images_count",external_images_count)
            base_url = "https://www.immobiliarefima.it"
            result_list = []
            for img in images:
                result = base_url + img
                result_list.append(result)
            item_loader.add_value("images",result_list)


        item_loader.add_value("city","Catania")
        item_loader.add_value("landlord_name","Immobiliare FIMA")
        item_loader.add_value("landlord_email","info@immobiliarefima.it")
        item_loader.add_value("landlord_phone","+39 095 2162121")
        item_loader.add_value("currency","EUR")


        


        

        yield item_loader.load_item()
