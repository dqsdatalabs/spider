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
    name = 'odeonimmobilier_fr'
    execution_type='testing' 
    country='france'
    locale='fr'   
    external_source = 'Odeonimmobilier_PySpider_france_fr'
    start_urls = ["https://odeonimmobilier.fr/location/location/"] 
    custom_settings = {
        "HTTPCACHE_ENABLED": False,
    }

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//h2[contains(@class,'property-row-title')]/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            title = item.xpath("./text()").get()
            yield Request(follow_url, callback=self.populate_item, meta={"title":title})
       
        
        next_page = response.xpath("//a[contains(@class,'next')]/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
            )

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.meta.get('title')
        if get_p_type_string(title):
            item_loader.add_value("property_type", get_p_type_string(title))
        else:
            return
        item_loader.add_value("external_link", response.url)
        external_id=response.xpath("//link[@rel='shortlink']/@href").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split("p=")[-1])

        item_loader.add_value("external_source", "Odeonimmobilier_PySpider_france")

        city = response.xpath("//dt[contains(.,'Emplacement')]/following-sibling::dd[1]/a[1]/text()").get()
        if city:
            item_loader.add_value("city", city.strip())

        title = response.xpath("//header/h1/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
            address = ""
            if " à " in title:
                address = title.split(" à ")[1].strip().split(" ")[0]
            elif "m²" in title:
                address = title.split("m²")[1].strip()
            else: 
                address = response.xpath("//dt[text()='Emplacement']/following-sibling::dd/a/text()").get()
            
            if address:
                item_loader.add_value("address", address)
        adrescheck=item_loader.get_output_value("address")
        if not adrescheck:
            item_loader.add_value("address",city.strip())
        
        description = " ".join(response.xpath("//h2[contains(.,'Description')]/following-sibling::*//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', '').replace("\n",""))

        square_meters = response.xpath("//dt[contains(.,'Surface')]/following-sibling::dd[1]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", "".join(filter(str.isnumeric, square_meters.split('m')[0].split(',')[0].strip())))

        
        room_count = response.xpath("//div[@class='property-overview']/dl/dt[contains(.,'Pièces')]//following-sibling::dd[1]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//dt[contains(.,'Salle de bain')]/following-sibling::dd[1]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        rent = response.xpath("//dt[contains(.,'Prix')]/following-sibling::dd[1]/text()").get()
        if rent:
            item_loader.add_value("rent", "".join(filter(str.isnumeric, rent.split('€')[0].split(',')[0].strip())))
            item_loader.add_value("currency", 'EUR')

        
        images = [response.urljoin(x) for x in response.xpath("//div[contains(@class,'gallery-preview-inner')]//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        energy_label = response.xpath("//div[@class='dpe dpeges-inline']//span[@class='diagnostic-number']/text()").get()
        if energy_label:
            energy_label = int(energy_label.strip())
            if energy_label <= 50:
                item_loader.add_value("energy_label", 'A')
            elif energy_label >= 51 and energy_label <= 90:
                item_loader.add_value("energy_label", 'B')
            elif energy_label >= 91 and energy_label <= 150:
                item_loader.add_value("energy_label", 'C')
            elif energy_label >= 151 and energy_label <= 230:
                item_loader.add_value("energy_label", 'D')
            elif energy_label >= 231 and energy_label <= 330:
                item_loader.add_value("energy_label", 'E')
            elif energy_label >= 331 and energy_label <= 450:
                item_loader.add_value("energy_label", 'F')
            elif energy_label >= 451:
                item_loader.add_value("energy_label", 'G')
        
        floor = response.xpath("//li[contains(.,'étage') and contains(.,'Au')]/text()").get()
        if floor:
            item_loader.add_value("floor", "".join(filter(str.isnumeric, floor.strip())))
        
        parking = response.xpath("//li[contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//figcaption[contains(.,'Balcon')]").get()
        balcony2 = response.xpath("//li[contains(.,'Balcon')]").get()
        if balcony or balcony2:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//li[contains(.,'Meublé')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//figcaption[contains(.,'Ascenseur')]").get()
        elevator2 = response.xpath("//li[contains(.,'Ascenseur')]").get()
        if elevator or elevator2:
            item_loader.add_value("elevator", True)

        terrace = response.xpath("//figcaption[contains(.,'Terrasse')]").get()
        terrace2 = response.xpath("//li[contains(.,'Terrasse')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        swimming_pool = response.xpath("//li[contains(.,'Piscine')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        washing_machine = response.xpath("//li[contains(.,'Lave-linge') or contains(.,'Lave-Linge')]").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)

        dishwasher = response.xpath("//li[contains(.,'Lave-Vaisselle') or contains(.,'Lave-vaisselle')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        item_loader.add_xpath("landlord_name", "//h3[@class='agent-small-title']/a/text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='agent-small-phone']/text()")
        item_loader.add_xpath("landlord_email", "//div[@class='agent-small-email']/a/text()")
      
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "commercial" in p_type_string.lower():
        return None
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("appartement" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "maison" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None