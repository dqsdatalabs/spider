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
    name = 'benedicte_immobilier_com' 
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Benedicte_Immobilier_PySpider_france'
    def start_requests(self, **kwargs):

        if not kwargs:
            kwargs = {"apartment":"IMMEUBLE", "house":"Maison"}

        for key, value in kwargs.items():
            formdata = {
                "achat_ou_loc": "louer",
                "type_immo": "",
                "budget_min": "",
                "budget_max": "",
                "localisation": ""
            }
            yield FormRequest("https://www.benedicte-immobilier.com/immobilier/",
                            callback=self.parse,
                            formdata=formdata,
                            meta={'property_type': key})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='annonces-infos']//a//@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source","Benedicte_Immobilier_PySpider_"+ self.country)

        title = response.xpath("//div[@class='immo-content text-justify']/p/text()").get()
        if title:
            item_loader.add_value("title", title)

        item_loader.add_xpath("external_id", "//div[@class='padding']/u[@class='ref']/strong/text()")

        rent = "".join(response.xpath("//div[@class='title-prix']//text()").get())
        if rent:
            price = rent.split('€')[0].strip()
            item_loader.add_value("rent_string", price)

        item_loader.add_value("currency", "EUR")
        address = "".join(response.xpath("(//div[@class='localisation']/text())[1]").getall())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip())

        description = " ".join(response.xpath("//div[@class='info-complementaire text-justify']//p//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.strip())

        images = [ response.urljoin(x) for x in response.xpath("//div[@class='carousel-inner']/div//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        uti = ""
        utilities = "".join(response.xpath("//div[@class='info-complementaire text-justify']/p[contains(.,'Charges')]//text()").getall())
        if utilities:
            uti = utilities.split('euros')[0].strip()
            item_loader.add_value("utilities", uti)


        deposit = "".join(response.xpath("//div[@class='info-complementaire text-justify']/p[contains(.,'Dépôt ')]//text()").getall())
        if deposit:
            item_loader.add_value("deposit", deposit.split('euros')[0].strip())
       
        room = ""
        room_count = "".join(response.xpath("//div[@class='carac-item'][contains(.,'de pièces')]//text()").getall())
        if room_count:
            room = room_count.strip().split(':')[1].strip()
            item_loader.add_value("room_count", room)

        bath = "".join(response.xpath("//div[@class='carac-item'][contains(.,'de bain')]//text()").getall())
        if bath:
            bath = bath.strip().split(':')[1].strip()
            item_loader.add_value("bathroom_count", bath)
        
        square_meters = " ".join(response.xpath("//div[@class='carac-item'][contains(.,'Surface')]//text()").getall()).strip()   
        if square_meters:
            meters =  square_meters.strip().split(':')[1].strip()
            item_loader.add_value("square_meters", meters)
        
        energy_label = " ".join(response.xpath("//div[@id='logementEco']/div/text()[2]").getall()).strip()   
        if energy_label:
            item_loader.add_value("energy_label", energy_label)


        item_loader.add_value("landlord_phone", "0 699 402 444")
        item_loader.add_value("landlord_name", "BENEDICTE Immobilier")
        item_loader.add_value("landlord_email", "contact@benedicte-immobilier.com")

        
        yield item_loader.load_item()

