# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math

class MySpider(Spider):
    name = 'ofim_fr' 
    execution_type='testing'
    country='france'
    locale='fr' 

    def start_requests(self):
        start_urls = [
            {"url": "https://www.ofim.fr/recherche.html?rp=1&rt=1&rc1=1&start=0",
            "prop_type": "apartment"},
            {"url": "https://www.ofim.fr/recherche.html?rp=1&rt=1&rc1=2&start=0",
            "prop_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={"type":url.get('type'), "property_type":url.get('prop_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 10)
        seen = False
        for item in response.xpath("//div[@class='list-group-item']//div[contains(@class,'col-lg-4')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 10 or seen:
            url = ''
            if response.meta.get('property_type') == 'apartment':
                url = f"https://www.ofim.fr/recherche.html?rp=1&rt=1&rc1=1&start={page}"
            else:
                url = f"https://www.ofim.fr/recherche.html?rp=1&rt=1&rc1=2&start={page}"
            yield Request(url, callback=self.parse, meta={"page": page+10, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_source", "Ofim_fr_PySpider_"+ self.country + "_" + self.locale)
        property_type = response.meta.get("property_type")
        
        item_loader.add_value("external_link", response.url)
        category = response.xpath("//tr[td[.='Catégorie']]/td[2]/text()").extract_first()
        if category:
            if "studio" in category.lower():
                property_type = "studio"
        item_loader.add_value("property_type", property_type)
        
        item_loader.add_xpath("title", "//div[@class='_readmorebox']/h3/text()")

        desc = "".join(response.xpath("//div[@class='_readmorebox']/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            
        rent =  "".join(response.xpath("normalize-space(//div[@class='titre ']/span[@class='price']/text())").extract())
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))

        meters = "".join(response.xpath("//span[@class='honoraires'] [contains(.,'Surface habitable :')]/text()").extract())
        
        if meters:
            s_meters = meters.split("Surface habitable :")[1].split("m²")[0].replace(",",".")
            item_loader.add_value("square_meters",math.ceil(float(s_meters)))
        else:
            meters2 = "".join(response.xpath("//div[@class='_readmorebox']/p/text()[contains(.,'Surface habitable :')]").extract())
            if meters2:
                s_meters2 = meters2.split("Surface habitable :")[1].split("m²")[0].replace(",",".")
                item_loader.add_value("square_meters",math.ceil(float(s_meters2)))
            elif "m2" in desc:
                sq_m=desc.split("m2")[0].strip().split(" ")[-1]
                if "," in sq_m:
                    sq_m=sq_m.split(",")[0]
                if sq_m.isdigit():
                    item_loader.add_value("square_meters", sq_m)        

        deposit = "".join(response.xpath("//span[@class='honoraires']/text()[contains(.,'garantie : ')]").extract())
        if deposit:
            item_loader.add_value("deposit", deposit.split("Dépôt de garantie :")[1].split("€")[0].replace(" ",""))
        utilities = "".join(response.xpath("//span[@class='honoraires']/text()[contains(.,'état des lieux :')]").extract())
        if utilities:
            item_loader.add_value("utilities", utilities.split("état des lieux :")[1].split("€")[0].replace(" ",""))

        desc = "".join(response.xpath("//div[@class='_readmorebox']/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        images = [x for x in response.xpath("//div[@class='item active']//img/@src[not(contains(.,'coeur-1-etat-repos-big@dpi4x.png') or contains(.,'data:image'))]").getall()]
        if images:
            item_loader.add_value("images", images)
        bathroom = response.xpath("//div[@class='_readmorebox']/p/text()[contains(.,'salle de bain')]").extract_first()
        if bathroom:
            bathroom = bathroom.split("salle de bain")[0].strip().split(" ")[-1].strip()
            if bathroom.isdigit():
                item_loader.add_value("bathroom_count", bathroom)

        ref = "".join(response.xpath("normalize-space(//div[@class='titre']/h1/text())").extract())
        if ref:
            ref = ref.split("réf.:")[1].strip()
            item_loader.add_value("external_id", ref)
        else:
            ref = "".join(response.xpath("normalize-space(//li[@class='active' and contains(.,'ref')]/text())").extract())
            if ref:
                ref = ref.split("ref.:")[1].strip()
                item_loader.add_value("external_id", ref)            

        room = "".join(response.xpath("//tr[td[.='Catégorie']]/td[2]/text()[contains(.,'chambre')]").extract())
        if room:
            room1 = " ".join(room.split(" ")[-3:]).split("chambres")[0]
            item_loader.add_value("room_count", room1.split(" ")[1])
        elif "chbres" in desc:
            room_count=desc.split("chbres")[0].strip().split(" ")[-1]
            if room_count.isdigit():
                item_loader.add_value("room_count", room_count)
        elif "studio" in property_type:    
            item_loader.add_value("room_count","1")
        item_loader.add_xpath("address", "//tr[td[.='Adresse']]/td[2]/text()")

        parking = response.xpath("//tr[td[.='Parking']]/td[2]/text()").get()
        if parking:
            if "non" in parking.lower():
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)

        balcony = response.xpath("//div[@class='_readmorebox']/p/text()[contains(.,'balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        item_loader.add_xpath("landlord_phone", "//tr[td[.='Tél. commercial']]/td[2]/text()")
        item_loader.add_value("landlord_name", "Ofim Fr")
      
        yield item_loader.load_item()