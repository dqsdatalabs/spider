# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
import re
import dateparser

class MySpider(Spider):
    name = 'faelensimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Faelensimmobilier_PySpider_france_fr'
    scale_separator ='.'
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.faelensimmobilier.com/site/produits.php?tri=&transac=Location&type=Maison&budget=5",
                "property_type" : "house"
            },
            {
                "url" : "https://www.faelensimmobilier.com/site/produits.php?tri=&transac=Location&type=Appartement&budget=5",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.faelensimmobilier.com/site/produits.php?tri=&transac=Location&type=Loft&budget=5",
                "property_type" : "apartment"
            },

        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type'), "referer": url.get('url')})


    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[contains(@class,'product-container')]/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )

        next_page = response.xpath("//a[@class='pagination__next']/@href").get()
        if next_page:
            referer = response.meta.get('referer')
            url = f"https://www.faelensimmobilier.com/site/{next_page}"
            headers = {
                "Accept": "*/*",
                "Referer": referer,
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.72 Safari/537.36",
                "X-Requested-With": "XMLHttpRequest",
            }
            yield Request(url, callback=self.parse, headers=headers, meta={"property_type" : response.meta.get("property_type"), "referer": referer})
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Faelensimmobilier_PySpider_"+ self.country + "_" + self.locale)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        rent="".join(response.xpath("//span[@class='prix']//text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))
        
        square_meters=response.xpath("//ul/li[contains(.,'Surface')]/span/text()").get()
        if square_meters:
            meters = square_meters.split('m²')[0].strip()
            item_loader.add_value("square_meters",int(float(meters)) )
        
        room_count=response.xpath("//ul/li[contains(.,'pièce')]/span/text()").get()
        room=response.xpath("//ul/li[contains(.,'chambre')]/span/text()").get()
        if room:
            item_loader.add_value("room_count", room)
        elif room_count:
            item_loader.add_value("room_count", room_count)
        
        address=response.xpath("//div[@class='description-container']/p/text()[1]").get()
        if address:
            item_loader.add_value("address", address.replace(',',' ').strip())
            if "," in address:
                item_loader.add_value("city", address.split(',')[0].strip())
            else:
                item_loader.add_value("city", address.split(' ')[0].strip())
                
        
        external_id=response.xpath("//span[@class='ref']//text()[not(contains(.,'www.lecomptoirindustriel.com'))]").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split('Réf.')[1].strip())

        desc="".join(response.xpath("//div[@class='description-container']/p/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        if "disponible" in desc.lower():
            available_date = desc.lower().split("disponible")[1].strip().replace(",",".").split(".")[0]
            match = re.search(r'(\d+/\d+/\d+)', available_date.replace("a",""))
            if match:
                try:
                    newformat = dateparser.parse(match.group(1), languages=['en']).strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", newformat)
                except: pass
            elif "le" in available_date:
                available_date = available_date.split("le")[1].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
                    
        images=[x for x in response.xpath("//div[@class='slider-detail']/a/div/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        name="".join(response.xpath("//span[@class='agent-infos']//text()").getall())
        if name:
            name=name.split(' ')
            if name[0]:
                item_loader.add_value("landlord_name", str(name[0]+" "+name[1]))
            else:
                item_loader.add_value("landlord_name", "FAELENS IMMOBILIER")
        else:
            item_loader.add_value("landlord_name", "FAELENS IMMOBILIER")
        
        
        phone=response.xpath("//span[@class='block']//text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone)
        else:
            item_loader.add_value("landlord_phone", "03 20 49 01 73 / 03 20 52 20 16")
            
        deposit=response.xpath("//ul/li[contains(.,'garantie')]/span/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split('€')[0].strip())
        else:
            deposit=response.xpath("//div[@class='description-container']/p/text()[contains(.,'Caution')]").get()
            if deposit:
                try:
                    deposit = deposit.split(":")[1].strip().split(" ")[0]
                except: deposit = deposit.split("euro")[0].strip().split(" ")[-1]
                item_loader.add_value("deposit", deposit)
            else:
                if "de garantie" in desc:
                    deposit = desc.split("de garantie")[1].split("euro")[0].replace(":","").strip().replace(" ","")
                    item_loader.add_value("deposit", deposit)
        
        utilities=response.xpath("//ul/li[contains(.,'charge')]/span/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split('€')[0].strip())
        else:
            utilities = response.xpath("//div[@class='description-container']/p/text()[contains(.,'Montant des charges')]").get()
            if utilities:
                utilities = utilities.split(":")[1].strip().split(" ")[0]
                item_loader.add_value("utilities", utilities)
            else:
                if "euros pour l'" in desc:
                    utilities = desc.split("euros pour l'")[0].strip().split(" ")[-1]
                    if "," in utilities:
                        item_loader.add_value("utilities", utilities.split(",")[0])
                    else:
                        item_loader.add_value("utilities", utilities)
                
        energy_label = response.xpath("//span[@class='dpeConso']/span/@class").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("dpeInfoConso")[1].strip().upper())

        furnished = response.xpath("//span[@class='type'][contains(.,'meublé')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        parking = response.xpath("//span[@class='type'][contains(.,'parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        yield item_loader.load_item()
