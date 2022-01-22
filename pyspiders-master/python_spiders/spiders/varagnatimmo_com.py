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
    name = 'varagnatimmo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.varagnatimmo.com/resultats?transac=location&type%5B%5D=appartement&budget_maxi=&surface_mini=&nb_piece=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.varagnatimmo.com/resultats?transac=location&type%5B%5D=maison&budget_maxi=&surface_mini=&nb_piece=",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            dont_filter=True,
                            meta={'property_type': url.get('property_type'), "base": item})

    # 1. FOLLOWING
    def parse(self, response):

        listing_script = response.xpath("normalize-space(//div[@class='properties-full']/script//text())").get()
        if listing_script:
            for i in range(1, len(listing_script.split('"lien":'))):
                url = listing_script.split('"lien":')[i].split(",")[0].replace('"', '').strip()
                follow_url = f"https://www.varagnatimmo.com/{url}"
                yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", "Varagnatimmo_PySpider_france")
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_xpath("external_id", "substring-after(//div[@class='col-sm-8']/h3/span[@class='small']/text(),'. ')")

        city = "".join(response.xpath("//li/address/text()").extract())
        if city:          
            item_loader.add_value("city", city.strip())
            item_loader.add_value("address",city.strip())

        rent = " ".join(response.xpath("normalize-space(//span[@class='property-thumb-info-label']/span[contains(@class,'price')]/text())").extract())
        if rent:
            price = rent.replace(" ","").replace("\xa0","").strip()
            item_loader.add_value("rent_string", price)
        rentcheck=item_loader.get_output_value("rent")
        if not rentcheck:
            rent1=response.xpath("//h3/span/text()").get()
            item_loader.add_value("rent_string",rent1)


        utilities = " ".join(response.xpath("//div[@class='panel-body']/ul/li[contains(.,'Charges')]/strong/text()").extract())
        if utilities:
            uti = utilities.strip().replace(" ","").replace("\xa0","").strip()
            item_loader.add_value("utilities", uti)

        deposit = " ".join(response.xpath("//div[@class='panel-body']/ul/li[contains(.,'Dépôt de garantie')]/strong/text()").extract())
        if deposit:
            deposit = deposit.strip().replace(" ","").replace("\xa0","").strip()
            item_loader.add_value("deposit", deposit)

        room_count = response.xpath("//div[@class='panel-body']/ul/li[contains(.,'Nombre de pièce')]/strong/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//div[@class='panel-body']/ul/li[contains(.,'Nombre de chambre')]/strong/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)

        item_loader.add_xpath("bathroom_count", "//div[@class='panel-body']/ul/li[contains(.,'Salle d')]/strong/text()")
        item_loader.add_xpath("floor", "//div[@class='panel-body']/ul/li[contains(.,'Numéro Etage')]/strong/text()")

        meters = " ".join(response.xpath("//div[@class='panel-body']/ul/li[contains(.,'Surface Habitable')]/strong/text()").extract())
        if meters:
            s_meters = meters.split("m²")[0].strip()
            item_loader.add_value("square_meters", s_meters.strip())

        description = " ".join(response.xpath("//div[@class='col-sm-8']/p/text()").getall())  
        if description:
            item_loader.add_value("description", description.strip())

        images = [x for x in response.xpath("//ul[@class='slides']/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_xpath("energy_label", "substring-before(substring-after(//img/@src[contains(.,'DPE')],'DPE_'),'.')")

        parking = response.xpath("//div[@class='panel-body']/ul/li[contains(.,'Parking')]/strong/text()").get()
        if parking:
            if parking != "0":
                item_loader.add_value("parking",False)
            else:
                item_loader.add_value("parking",True)

        elevator = response.xpath("//div[@class='panel-body']/ul/li[contains(.,'Ascenseur')]/strong/text()").get()
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator",False)
            elif "oui" in elevator.lower():
                item_loader.add_value("elevator",True)

        item_loader.add_value("landlord_phone", "04 77 96 81 00  ")
        item_loader.add_value("landlord_name", "GESTION IMMOBILIÈRE VARAGNAT")
        item_loader.add_value("landlord_email", "varagnatimmo@wanadoo.fr")
        
        yield item_loader.load_item()