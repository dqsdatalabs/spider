# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import json2html
import re
from datetime import datetime
class MySpider(Spider):
    name = 'letandemimmobilier_com'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.letandemimmobilier.com/recherche,basic.htm?idqfix=1&idtt=1&idtypebien=1&px_loyermax=Max&px_loyermin=Min&saisie=O%c3%b9+d%c3%a9sirez-vous+habiter+%3f&surfacemax=Max&surfacemin=Min&tri=d_dt_crea&", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            })

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[contains(@class,'recherche-annonces')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get("property_type"))
        item_loader.add_value("external_source", "Letandemimmobilier_PySpider_"+ self.country + "_" + self.locale)

        # title = "".join(response.xpath("//h1/text()").extract())
        # item_loader.add_value("title", title.strip())
        title = re.sub('\s{2,}', ' ', ("".join(response.xpath("//h1[@itemprop='name']//text()").getall()).replace("\n",""))).strip()
        if title :            
            item_loader.add_value("title", title)

            address=title.split("- ")[1].split("(")[0]
            zipcode=title.split("(")[1].split(")")[0]
            if address:
                # item_loader.add_value("city", address)
                item_loader.add_value("address",address)
            if zipcode:
                item_loader.add_value("zipcode",zipcode)
        
        latitude_longitude=response.xpath("//script[contains(.,'LATITUDE')]/text()").get()
        if latitude_longitude:
            latitude_longitude=latitude_longitude.split("ANNONCE:")[1]
            latitude=latitude_longitude.split('LATITUDE: "')[1].split('"')[0]
            longitude=latitude_longitude.split('LONGITUDE: "')[1].split('"')[0]
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        external_id = response.xpath("//div/span[contains(.,'Référence')]/text()").extract_first()
        if external_id :
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id",external_id)
       
        price = "".join(response.xpath("//div[contains(@class,'h1-like')]//text()").extract())
        if price :
            item_loader.add_value("rent_string", price.split("CC")[0].replace(" ",""))

        desc = "".join(response.xpath("//p[@itemprop='description']//text()").extract())
        if desc :
            item_loader.add_value("description", desc.strip()) 
            if "balcon" in desc :
                item_loader.add_value("balcony", True)
            if "parking " in desc :
                item_loader.add_value("parking", True)
        
        square_meters = response.xpath("//li[contains(.,'Surface')]/div[2]/text()").extract_first()
        if square_meters :           
            item_loader.add_value("square_meters", square_meters.split("m")[0])
    
        floor = response.xpath("//li[contains(.,'Etage')]/div[2]/text()").extract_first()
        if floor :  
            item_loader.add_value("floor", floor.strip())

        deposit = response.xpath("//div[contains(.,'Dépôt de garantie')]/strong/text()").extract_first()
        if deposit :  
            dp=deposit.split(":")[1].split("€")[0].strip()
            item_loader.add_value("deposit", str(dp))
        
        utilities=response.xpath("//p[contains(@class,'prix-honoraires')]/strong/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(":")[1].split("€")[0].strip())
        
        room_count = response.xpath("//li[contains(.,'Pièce')]/div[2]/text()").extract_first()
        if room_count :
            item_loader.add_value("room_count", room_count)
        
        bathroom_count=response.xpath("//li[contains(.,'Salle')]/div[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        elevator = response.xpath("//li[contains(.,'Ascenseur')]/div[2]/text()").extract_first()
        if elevator :
            item_loader.add_value("elevator", True)
        
        terrace = response.xpath("//li[contains(.,'Terrasse')]/div[2]/text()").extract_first()
        if terrace :
            item_loader.add_value("terrace", True)

        balcony = response.xpath("//li[contains(.,'Balcons')]/div[2]/text()").extract_first()
        if balcony :        
            item_loader.add_value("balcony", True)
        parking = response.xpath("//li[contains(.,'Parking')]/div[2]/text()").extract_first()
        if parking :        
            item_loader.add_value("parking", True)

        energy_label = response.xpath("//div/p[contains(.,'Consommations énergétiques')]/following-sibling::div/div[@class='row-fluid']/div[2]/text()").extract_first()
        if energy_label :
            item_loader.add_value("energy_label", energy_label.strip())
        
        item_loader.add_value("landlord_name", "LE TANDEM IMMOBILIER")
        item_loader.add_value("landlord_phone", "01 61 39 17 32")
        item_loader.add_value("landlord_email", "letandemimmobilier@orange.fr")
        
        images = [x for x in response.xpath("//div[@id='slider']//img/@src").extract()]
        if images:
            item_loader.add_value("images", images)

        a_date = response.xpath("//div/p[contains(.,'Disponible le')]/text()").extract_first()
        if a_date:
            oldformat = a_date.split(":")[1].strip()
            datetimeobject = datetime.strptime(oldformat,'%d/%m/%Y')
            newformat = datetimeobject.strftime('%Y-%m-%d')
            item_loader.add_value("available_date", newformat)
            
        yield item_loader.load_item()