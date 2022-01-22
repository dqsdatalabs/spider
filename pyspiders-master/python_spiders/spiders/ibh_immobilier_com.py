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
    name = 'ibh_immobilier_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source = "Ibh_immobilier_PySpider_france_fr"
    start_urls = ['https://www.ibh-immobilier.com/location/1']  # LEVEL 1
    custom_settings = {
        "HTTPCACHE_ENABLED":False
    }

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//li[@class='properties-list-v2__item']//a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)

        next_page = response.xpath("//*[@class='paging-v1__svg']/parent::a/@href").get()
        if next_page:
            next_page = "https://www.ibh-immobilier.com" + next_page
            yield Request(next_page, callback=self.parse)
            

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        prop_type=response.url
        if prop_type and "appartement" in prop_type:
            item_loader.add_value("property_type", "apartment")
        elif prop_type and "loft" in prop_type:
            item_loader.add_value("property_type", "studio")
        else:
            return

        title = response.xpath("//title//text()").get()

        if "parking" in title.lower():
            return
        if title:
            item_loader.add_value("title", title.replace("\u00e9","").replace("\u00e8","").replace("\u00b2","").replace("\u00a0",""))

        description = " ".join(response.xpath("//div[@class='properties-details-infos-v1__description']//p//text()").getall())
        if description:
            item_loader.add_value("description", description.replace("\u00e9","").replace("\u00e8","").replace("\u00b2","").replace("\u00a0","").replace("\u0153","").replace("\u00e0",""))

        address = response.xpath("(//li[@class='breadcrumb-v1__item']//a[contains(@class,'breadcrumb-v1__link')]//text())[4]").get()
        if address:
            item_loader.add_value("city", address)
            item_loader.add_value("address", address)

        external_id = response.xpath("//div[text()='Références']/following-sibling::div/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)

        zipcode = response.xpath("//div[text()='Code postal']/following-sibling::div/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)

        square = response.xpath("//li[@class='properties-details-infos-v1__item']//div[contains(.,'Surface habitable')]//following-sibling::div//text()").get()
        if square:
            square_meters =square.split("m²")[0]
            item_loader.add_value("square_meters", square_meters)
        else:
            square = response.xpath(" //li[@class='properties-details-infos-v1__item']//div[contains(.,'Surface loi Carrez')]//following-sibling::div//text()").get()
            if square:
                square_meters =square.split("m²")[0]
                item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//li[@class='properties-details-infos-v3__item']//div[contains(.,'Nombre de pièces')]//following-sibling::div//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)  
        else:
            room_count = response.xpath("//li[@class='properties-details-infos-v1__item']//div[contains(.,'Nombre de chambre')]//following-sibling::div//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//div[text()='Nb de salle de bains']/following-sibling::div/text()").get()
        if bathroom_count :
            item_loader.add_value("bathroom_count", bathroom_count)

        price = response.xpath("//span[@class='properties-price__value']/text()").get()
        if price:
            rent = price.split("€")[0].replace(" ","")
            item_loader.add_value("rent",rent)

        item_loader.add_value("currency", "EUR")

        utilities = response.xpath("//div[text()='Charges locatives (provision donnant lieu à régularisation annuelle)']/following-sibling::div/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0])

        deposit = response.xpath("//div[text()='Dépôt de garantie TTC']/following-sibling::div/text()").get()
        if deposit:
            if " " in deposit:
                deposit = deposit.replace(" ","") 
                deposit= deposit.split("€")[0]
                item_loader.add_value("deposit", deposit)
            else:
                deposit= deposit.split("€")[0]
                item_loader.add_value("deposit", deposit)

        floor = response.xpath("//li[@class='properties-details-infos-v1__item']//div[contains(.,'Nombre de niveaux')]//following-sibling::div//text()").get()
        if floor:
            item_loader.add_value("floor", floor)

        furnished = response.xpath("//li[@class='properties-details-infos-v1__item']//div[contains(.,'Meublé')]//following-sibling::div//text()").get()
        if furnished and "oui" in  furnished.lower():
            item_loader.add_value("furnished", True)
        else:
            item_loader.add_value("furnished", False)
        
        elevator = response.xpath("//li[@class='properties-details-infos-v1__item']//div[contains(.,'Ascenseur')]//following-sibling::div//text()").get()
        if elevator and "oui" in  elevator.lower():
            item_loader.add_value("elevator", True)
        else:
            item_loader.add_value("elevator", False)

        balcony = response.xpath("//li[@class='properties-details-infosmisc-v1__item']//div[contains(.,'Balcon')]//following-sibling::div//text()").get()
        if balcony and "oui" in  balcony.lower():
            item_loader.add_value("balcony", True)
        else:
            item_loader.add_value("balcony", False)
       
        latitude = response.xpath("//div[@class='module-map-poi']//@data-lat").get()
        if latitude:
            item_loader.add_value("latitude", latitude)

        longitude = response.xpath("//div[@class='module-map-poi']//@data-lng").get()
        if longitude:
            item_loader.add_value("longitude", longitude)

        images = [x for x in response.xpath("//div[@class='swiper-slide']//img//@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
  
        item_loader.add_value("landlord_name", "Ibh Immobilier")
        item_loader.add_value("landlord_phone","04 91 22 15 15")

        yield item_loader.load_item()