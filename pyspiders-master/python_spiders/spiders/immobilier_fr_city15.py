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
    name = 'immobilier_fr_city15'
    execution_type='testing'
    country='france'
    locale='fr' 
    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.city-15-immobilier.fr/catalog/advanced_search_result.php?action=update_search&search_id=&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_28_tmp=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&keywords=&C_30_MIN=&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MIN=&C_33_MAX=&C_34_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=&C_38_MAX=&C_38_MIN=&C_38_search=COMPRIS&C_38_type=NUMBER&C_47_type=NUMBER&C_47_search=COMPRIS&C_47_MIN=&C_94_type=FLAG&C_94_search=EGAL&C_94=",
                "property_type" : "apartment"
            },
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[contains(.,'Découvrir')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
      
        item_loader.add_value("external_source", "Immobilier_fr_city15_PySpider_"+ self.country + "_" + self.locale)
        title = re.sub('\s{2,}', ' ', ("".join(response.xpath("//div[@class='title-product']/h1/text()").getall()).replace("\n",""))).strip()
        if title :            
            item_loader.add_value("title", title)
        
        address = response.xpath("//div[@class='title-product']/h1/span/text()").get()
        if address:
            item_loader.add_value("address", address)
            
            
        zipcode = response.xpath("//li[contains(.,'Code')]//div[2]//text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        
        city = response.xpath("//li[contains(.,'Ville')]//div[2]//text()").get()
        if city:
            item_loader.add_value("city", city)
        
        floor = response.xpath("//li[contains(.,'Etage')]//div[2]//text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        utilities = response.xpath("//li[contains(.,'charges')]//div[2]//text()").get()
        if utilities:
            utilities = utilities.split(" ")[0]
            item_loader.add_value("utilities", utilities)
        
        deposit = response.xpath("//li[contains(.,'Garantie')]//div[2]//text()").get()
        if deposit: 
            deposit = deposit.split(" ")[0]
            item_loader.add_value("deposit", deposit) 
        
        room_count = response.xpath("//li[contains(.,'Chambre')]//div[2]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        roomcheck=item_loader.get_output_value("room_count")
        if not roomcheck:
            room1=response.xpath("//li[contains(.,'pièce(s)')]//div[2]//text()").get()
            if room1:
                room=re.findall("\d+",room1)
                item_loader.add_value("room_count", room) 

        
        
        bathroom_count = response.xpath("//li[contains(.,'Salle')]//div[2]//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        square_meters = response.xpath("//li[contains(.,'Surface')]//div[2]//text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        
        available_date = response.xpath("//li[contains(.,'Disponibilité')]//div[2]//text()").extract_first()
        if available_date :
            match = re.search(r'(\d+/\d+/\d+)', available_date)
            if match:
                newformat = dateparser.parse(match.group(1), languages=['en']).strftime("%Y-%m-%d")
                item_loader.add_value("available_date", newformat)
       
        energy_label = response.xpath("//li[contains(.,'Conso')]//div[2]//text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
            
        elevator = response.xpath("//li[contains(.,'Ascenseur')]//div[2]//text()").get()
        if elevator and "oui" in elevator.lower():
            item_loader.add_value("elevator", True)
            
        balcony = response.xpath("//li[contains(.,'parking')]//div[2]//text()").get()
        if balcony and balcony!='0':
            item_loader.add_value("balcony", True)

        parking = response.xpath("//li[contains(.,'balcon')]//div[2]//text()").get()
        if parking and parking!='0':
            item_loader.add_value("parking", True)
            
        external_id = response.xpath("substring-after(//span[@itemprop='name' and contains(.,'Ref')]/text(),':')").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        price = response.xpath("//span[@class='alur_loyer_price']/text()").extract_first()
        if price:
            price = price.split("€")[0].strip().split(" ")[-1].replace("\xa0","")
            item_loader.add_value("rent", price.strip())
            item_loader.add_value("currency", "EUR")

        desc = "".join(response.xpath("//div[@class='description-product']/text()").extract())
        if desc :
            item_loader.add_value("description", desc.strip())
        
        furnished = response.xpath("//li[@title='Balcon']/div[@class='detail-sign-val']/text()").extract_first()
        if furnished : 
            if furnished=="oui":  
                item_loader.add_value("furnished", True)

        
        item_loader.add_value("landlord_name", "City 15 Immobilier")
        item_loader.add_value("landlord_phone", "09 83 79 77 06")
        
        images = [response.urljoin(x) for x in response.xpath("//div[@id='slider_product']//@src").extract()]
        if images:
            item_loader.add_value("images", images)

        yield item_loader.load_item()