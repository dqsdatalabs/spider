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
import dateparser

class MySpider(Spider):
    name = 'gedim_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://fr.foncia.com/location/ardennes-08--aube-10--marne-51--haute-marne-52--meurthe-et-moselle-54--meuse-55--moselle-57--bas-rhin-67--haut-rhin-68--vosges-88/appartement--chambre--appartement-meuble",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://fr.foncia.com/location/ardennes-08--aube-10--marne-51--haute-marne-52--meurthe-et-moselle-54--meuse-55--moselle-57--bas-rhin-67--haut-rhin-68--vosges-88/maison",
                    
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

    def parse(self, response):
        
        for item in response.xpath("//div[@data-content='search_results_container']//a[@class='TeaserOffer-ill']/@href").extract():
            follow_url = response.urljoin(item)            
            yield Request(follow_url,
                            callback=self.jump,
                            meta={'property_type': response.meta.get('property_type')})
            
        next_page = response.xpath("//a[.='Suivante >']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )

    # 1. FOLLOWING
    # def jump(self, response):
        
    #     for item in response.xpath("//h3[@class='TeaserOffer-title']/a/@href").extract():
    #         follow_url = response.urljoin(item)
    #         yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

        
    #     next_page = response.xpath("//a[.='Suivante >']/@href").get()
    #     if next_page:
    #         yield Request(
    #             url=response.urljoin(next_page),
    #             callback=self.jump,
    #             meta={'property_type': response.meta.get('property_type')}
    #         )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Gedim_PySpider_"+ self.country + "_" + self.locale)

        prop_type = response.meta.get('property_type')
        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else:
            return

        item_loader.add_value("external_link", response.url)
        
        rent=response.xpath("normalize-space(//p[@class='OfferTop-price']/text())").get()
        if rent:
            item_loader.add_value("rent_string", rent.split(".")[0].replace(" ","")+"€")
        
        sq_meter=response.xpath("//ul/li/span[contains(.,'Surface')]//following-sibling::strong/text()").get()
        square_meters="".join(response.xpath("//p[@class='MiniData-item'][contains(.,'m2')]/text()").getall())
        if square_meters and "." in square_meters:
            item_loader.add_value("square_meters", square_meters.split(".")[0].strip())
        elif square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        elif sq_meter and "." in sq_meter:
            item_loader.add_value("square_meters", sq_meter.split(".")[0].strip())
        elif sq_meter:
            item_loader.add_value("square_meters", sq_meter.split("m")[0].strip())
            
        room_count=response.xpath("normalize-space(//p[@class='MiniData-item'][contains(.,'pièce')]/text())").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])
        
        
        address = response.xpath("//p[@class='OfferTop-loc']/text()[2]").get()
        if address:
            address = address.strip().replace(' ', '').replace('\n', ' ').replace('(', ' (')
            zipcode = address.split('(')[1].split(')')[0].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split("(")[0].strip().split(" ")[-1])
            item_loader.add_value("zipcode", zipcode)
        
        available_date = "".join(response.xpath("//p[@class='OfferTop-dispo']/text()").getall())
        if available_date:
            available_date = available_date.replace("Disponible","").replace("le","")
            
            date_parsed = dateparser.parse(
                    available_date, date_formats=["%d/%m/%Y"]
                )
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        energy = response.xpath("//p[@class='positionLeft']/img/@src").get()
        if energy:
            energy_label = energy.split("foncia.net/")[1].split("/")[0]
            if energy_label.isdigit():
                item_loader.add_value("energy_label", energy_label_calculate(energy_label))
        
        floor = response.xpath("//ul/li/span[contains(.,'étages')]/following-sibling::strong/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        external_id=response.xpath("//div[@class='OfferDetails']/section/p/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(".")[1].strip())
        
        title=response.xpath("//div[@class='OfferTop-head']/h1/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title))
        
        desc="".join(response.xpath("//div[@data-widget='ToggleBlockMobile']//text()").getall())
        desc2="".join(response.xpath("//div[@class='OfferDetails-content']/p[1]/text()").getall())
        if desc or desc2:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc2.replace("\n","")+desc.replace("\n","")))
        
        if " meubl\u00e9" in desc or" meubl\u00e9" in desc2:
            item_loader.add_value("furnished", True)
        elif " meuble" in desc or" meuble" in desc2:
            item_loader.add_value("furnished", True)
        
        if "lave vaisselle" in desc or "lave vaisselle" in desc2:
            item_loader.add_value("dishwasher", True)
        
        if "piscine" in desc or "piscine" in desc2:
            item_loader.add_value("swimming_pool", True)
        
        if "machine \u00e0 laver" in desc or "machine \u00e0 laver" in desc2:
            item_loader.add_value("washing_machine", True)
        
        if "salle" in desc2:
            bathroom = desc2.split("salle")[0].strip().split(" ")[-1]
            if bathroom.isdigit():
                item_loader.add_value("bathroom_count", bathroom)
            elif "une" in bathroom:
                item_loader.add_value("bathroom_count", "1")

        images=[x for x in response.xpath("//ul[@class='OfferSlider-main-slides']/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        else:
            images=[x for x in response.xpath("//div[@class='OfferSlider']//img/@src").getall()]
            if images:
                item_loader.add_value("images", images)
            


        phone=response.xpath("//div[@class='OfferContact-content']/p/a/span[contains(.,'+')]/text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone)
        
        charges=response.xpath("normalize-space(//span[contains(.,'Honoraires charge')]/following-sibling::*/text())").get()
        if charges:
            charges = charges.split("€")[0].split(".")[0].strip()
            if charges != "0":
                item_loader.add_value("utilities", charges)
        
        parking = response.xpath("//span[contains(.,'Parking')]/following-sibling::*/text()").get()
        if parking:
            item_loader.add_value("parking", True)

        elevator = response.xpath("//li[contains(.,'Ascenseur') or contains(.,'ascenseur')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        deposit = response.xpath("//span[contains(.,'Dépôt')]/following-sibling::*/text()").get()
        if deposit:
            deposit = deposit.split("€")[0].split(".")[0].strip()
            if deposit != "0":
                item_loader.add_value("deposit", deposit)
        
        item_loader.add_value("landlord_name", "FONCIA")
        
        status=response.xpath("//ul/li/span[contains(.,'Type')]/following-sibling::strong/text()").get()
        if status and "parking" not in status.lower():
            yield item_loader.load_item()

def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label