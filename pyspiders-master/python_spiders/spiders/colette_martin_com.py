# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
class MySpider(Spider):
    name = 'colette_martin_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://colette-martin.com/recherche.php?type=location&lst_type=appartement&secteur=&prix_min=0&prix_max=1000",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://colette-martin.com/recherche.php?type=location&lst_type=maison&secteur=&prix_min=0&prix_max=1000",
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
        for item in response.xpath("//div[@class='bien-resultat']/a/@href").getall():
            follow_url = "https://colette-martin.com" + item.strip()
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
        
        next_page = response.xpath("//a[.='»']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta['property_type']}
            )

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Colette_Martin_PySpider_france")
        external_id =response.xpath("substring-after(//div/h1/span//text()[contains(.,'Ref')],':')").extract_first()
        if external_id:
            item_loader.add_value("external_id",external_id.strip())
        title =response.xpath("//div/h1/text()[normalize-space()]").extract_first()
        if title:
            item_loader.add_value("title",title )
            address = title.strip().split(" ")[-2]+" "+title.strip().split(" ")[-1]
            item_loader.add_value("city",address)
            item_loader.add_value("address",address)
                 
        rent =" ".join(response.xpath("//div/h3[@class='bien-detail-prix']//text()").extract())
        if rent:     
           item_loader.add_value("rent_string", rent.replace(" ","").replace("\xa0",""))   
            
        room_count = response.xpath("//ul[@id='detail']/li[contains(.,'chambre')]/span//text()[.!='-- ']").extract_first() 
        if room_count:   
            item_loader.add_value("room_count",room_count.strip())
        elif title and "STUDIO" in title:
            item_loader.add_value("room_count","1")

        square ="".join( response.xpath("//ul[@id='detail']/li[contains(.,'Surface habitable')]/span//text()").extract() )  
        if square:
            square_meters =  square.split("m")[0].strip().replace(",",".")
            item_loader.add_value("square_meters", int(float(square_meters))) 
      
        utilities =" ".join(response.xpath("substring-after(//span[@class='prix_honoraires']//text()[contains(.,'Charges')],'Charges ')").extract())       
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[0].replace(" ","")) 
        deposit =" ".join(response.xpath("//p[@id='descriptif']//text()[contains(.,'de garantie')]").extract())       
        if deposit:
            item_loader.add_value("deposit",deposit.split("de garantie")[1].split("euro")[0].replace(" ","")) 
        
        available_date = response.xpath("//p[@id='descriptif']//text()[contains(.,'Disponible le')]").extract_first() 
        if available_date:
            date_parsed = dateparser.parse(available_date.split("Disponible le")[1].strip().split(" ")[0], languages=['fr'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        desc = " ".join(response.xpath("//p[@id='descriptif']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        energy = response.xpath("//div[div[@id='detailBienDPE']]/p[contains(.,'Valeur')]//text()").extract_first()       
        if energy:
            item_loader.add_value("energy_label", energy_label_calculate(energy.split(":")[1].strip().split(".")[0])) 
       
        item_loader.add_value("landlord_phone", "03 80 30 62 00")
        item_loader.add_value("landlord_email", "agence@colette-martin.com")
        item_loader.add_value("landlord_name", "COLETTE MARTIN") 

        images = [response.urljoin(x)for x in response.xpath("//div[@id='photos']//img/@src").extract()]
        if images:
                item_loader.add_value("images", images)
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