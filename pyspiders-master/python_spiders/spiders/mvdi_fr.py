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
    name = 'mvdi_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.mvdi.fr/recherche,incl_recherche_prestige_ajax.htm?surfacemin=Min&surfacemax=Max&surf_terrainmin=Min&surf_terrainmax=Max&px_loyermin=Min&px_loyermax=Max&idqfix=1&idtt=1&pres=prestige&idtypebien=1&lang=fr&tri=d_dt_crea&_=1611216690603",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.mvdi.fr/recherche,incl_recherche_prestige_ajax.htm?surfacemin=Min&surfacemax=Max&surf_terrainmin=Min&surf_terrainmax=Max&px_loyermin=Min&px_loyermax=Max&idqfix=1&idtt=1&pres=prestige&idtypebien=2&lang=fr&tri=d_dt_crea&_=1611216690597",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='span8']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", "Mvdi_PySpider_france")
        title =" ".join(response.xpath("//div/h1//text()").extract())
        if title:
            title = re.sub("\s{2,}", " ", title)
            item_loader.add_value("title", title)  
            address = title.split(" -")[-1] 
            item_loader.add_value("address",address )
            if "(" in address:
                city = address.split("(")[0].strip()
                zipcode = address.split("(")[1].split(")")[0].strip() 
                item_loader.add_value("city",city ) 
                item_loader.add_value("zipcode",zipcode ) 
        external_id = response.xpath("//div[@class='span6 margin-top-20']/div[@class='bloc-detail-reference']/span/text()").extract_first()
        if external_id: 
            item_loader.add_value("external_id",external_id.split(" :")[-1].strip())    
  
        room_count = response.xpath("//li[div[contains(.,'Chambre')]]/div[2]/text()").extract_first()
        if room_count:
            item_loader.add_value("room_count",room_count) 
        else:
            room_count = response.xpath("//li[div[.='Pièce']]/div[2]/text()").extract_first()
            if room_count:
                item_loader.add_value("room_count",room_count) 

        item_loader.add_xpath("floor", "//li[div[.='Etage']]/div[2]/text()")
 
        item_loader.add_xpath("bathroom_count", "//li[div[contains(.,'Salle d')]]/div[2]/text()")
        rent =" ".join(response.xpath("//div[contains(@class,'hidden-desktop')]//div[span[@itemprop='price']]//text()").extract())
        if rent:     
            item_loader.add_value("rent_string",rent.replace('\xa0', '').replace(' ','').replace(",",""))  
  
        deposit =response.xpath("//strong[contains(.,'Dépôt de garantie')]/text()").extract_first()
        if deposit:   
            deposit = deposit.split("€")[0].split("Dépôt de garantie")[1].replace(":","").strip()
            item_loader.add_value("deposit", int(float(deposit.replace(",","."))))  
                
        square =response.xpath("//li[div[.='Surface']]/div[2]/text()").extract_first()
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters",int(float(square_meters.replace(",",".")))) 

        energy_label =response.xpath("//div[@class='diagramme'][img[contains(@src,'diag_dpe')]]/p[contains(@class,'diagLettre diag')]/text()").extract_first()    
        if energy_label:
            item_loader.add_value("energy_label",energy_label.strip())  
   
        terrace =response.xpath("//li[div[.='Terrasse']]/div[2]/text()").extract_first()    
        if terrace:
            if "non" in terrace.lower():
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)
        balcony =response.xpath("//li[div[contains(.,'Balcon')]]/div[2]/text()").extract_first()    
        if balcony:
            if "non" in balcony.lower():
                item_loader.add_value("balcony", False)
            else:
                item_loader.add_value("balcony", True)

        elevator =response.xpath("//li[div[.='Ascenseur']]/div[2]/text()").extract_first()    
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
        furnished =response.xpath("//li[div[.='Meublé']]/div[2]/text()").extract_first()    
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
    
        desc = " ".join(response.xpath("//p[@itemprop='description']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
              
        images = [response.urljoin(x) for x in response.xpath("//div[@id='slider']/a/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)
            
        utilities = response.xpath("//li[contains(.,'pour charge')]//text()[contains(.,'€')]").get()
        if utilities:
            utilities = utilities.split(":")[1].split("€")[0].strip()
            item_loader.add_value("utilities", utilities)
        
        import dateparser
        available_date = response.xpath("//p/text()[contains(.,'Disponible')]").get()
        if available_date:
            available_date = available_date.replace("Disponible","").replace("au","").replace("le","").replace(":","").strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        latitude_longitude = response.xpath("//script[contains(.,',LATITUDE: \"')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split(',LATITUDE: "')[2].split('"')[0]
            longitude = latitude_longitude.split(',LONGITUDE: "')[2].split('"')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        item_loader.add_value("landlord_name", "MVDI")
        item_loader.add_value("landlord_phone", "02 40 96 26 87")
        item_loader.add_value("landlord_email", "referencementprestataire@gmail.com")
               
        yield item_loader.load_item()