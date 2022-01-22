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
    name = 'sgl_immo_com' 
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'   
    external_source = "Sgl_Immo_PySpider_france"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://sgl-immo.com/location-appartement/",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://sgl-immo.com/location-maison/",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='sgl-card--content p-4']//p/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        
        title = response.xpath("//h1[@class='sgl-bien--h1']//strong/text()").get()
        if title:
            item_loader.add_value("title", title.split())

        meters = "".join(response.xpath("//li[contains(.,'habitable')]/text()[1]").getall())
        if meters:
            item_loader.add_value("square_meters", meters.split("m")[0].strip())

        room_count = "".join(response.xpath("//li/text()[contains(.,'chambre')]").getall())
        if room_count:
            item_loader.add_xpath("room_count", room_count.split("chambre")[0])
        else:
            room_count = "".join(response.xpath("//li/text()[contains(.,'pièce')]").getall())
            if room_count:
                item_loader.add_xpath("room_count", room_count.split("pièce")[0])
    

        description = " ".join(response.xpath("//section[@class='sgl-bien--description px-3']//p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())

        rent = " ".join(response.xpath("//p[@id='sgl-coutBien--prix']/text()").getall())
        if rent:
            price = rent.replace("\xa0","")
            item_loader.add_value("rent_string",price.strip())

        images = [x for x in response.xpath("//div[@class='sgl-bien--sliders__thumbnails']//img/@data-lazy-src").getall()]
        if images:
            item_loader.add_value("images", images)

        deposit = "".join(response.xpath("//p[span[contains(.,'Dépot de garantie')]]/span[2]/text()").getall())
        if deposit:
            item_loader.add_value("deposit", deposit.strip())

        address = "".join(response.xpath("//h1[@class='sgl-bien--h1']//span[2]/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(" ")[0])
            item_loader.add_value("zipcode", address.split(" ")[-1].strip())

        external_id = "".join(response.xpath("//h1[@class='sgl-bien--h1']//span/text()[contains(.,'Réf.')]").getall())
        if external_id:
            item_loader.add_value("external_id", external_id.split("Réf.")[-1].strip())

        floor = "".join(response.xpath("//li[contains(.,'étage')]/text()[1]").getall())
        if floor:
            item_loader.add_value("floor", floor.strip())
        
        energy_label = response.xpath("//ul[@class='sgl-bien--conso d-flex align-items-center pl-0']//li[contains(@class,'active')]/text()").extract_first()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
            

        utilities = "".join(response.xpath("//p[span[contains(.,'Provision charges')]]/span[2]/text()").getall())
        if utilities:
            item_loader.add_value("utilities", utilities.strip())

        elevator = "".join(response.xpath("//li/text()[contains(.,'Ascenseur')]").getall())
        if elevator:
            item_loader.add_value("elevator", True)
      
        balcony = "".join(response.xpath("//li/text()[contains(.,'Balcon')]").getall())
        if balcony:
            item_loader.add_value("balcony", True)

        parking = "".join(response.xpath("//li/text()[contains(.,'parking')]").getall())
        if parking:
            item_loader.add_value("parking", True)
            # else:
            #     item_loader.add_value("parking", False)

        furnished = "".join(response.xpath("//li/text()[contains(.,'meublé')]").getall())
        if furnished:
            if "non-meublé" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)

        item_loader.add_xpath("landlord_phone", "//div[@class='col-12 col-md-4 col-lg-7 col-xl-6 px-0 pl-md-3 active-sticky']/a[contains(@href,'tel')]/text()")
        item_loader.add_xpath("landlord_name", "//div[@class='col-12 col-md-4 col-lg-7 col-xl-6 px-0 pl-md-3 active-sticky']/p[2]/text()")

        yield item_loader.load_item()


