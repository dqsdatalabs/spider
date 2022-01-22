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
    name = 'rouengauche_stephaneplazaimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {"url": "https://rouengauche.stephaneplazaimmobilier.com/location/appartement", "property_type": "apartment"},
            {"url": "https://rouengauche.stephaneplazaimmobilier.com/search/rent?target=rent&type[]=1&agency_id=380&sort=&idagency=253546&markers=true&limit=50&page=0", "property_type": "apartment"},
	        {"url": "https://rouengauche.stephaneplazaimmobilier.com/location/maison", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={"property_type": url.get('property_type')}
                        )

    # 1. FOLLOWING
    def parse(self, response):

        data = response.xpath("//div[@class='acheter-right']/div//a[@class='cover-link']/@href").extract()
        for item in data:
            follow_url = item
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//title")
        
        lat = response.meta.get("lat")
        lng = response.meta.get("lng")
        images = response.meta.get("images")

        item_loader.add_value("latitude", str(lat))
        item_loader.add_value("longitude", str(lng))
        
        item_loader.add_value("images",images)

        item_loader.add_value("external_source", "Rouengauche_Stephaneplazaimmobilier_PySpider_france")
        
        ext_id =response.xpath("//ul/li[contains(.,'Référence')]//text()").extract_first()
        if ext_id:
            item_loader.add_value("external_id",ext_id.replace("Référence","").strip() )    
        address =response.xpath("//div[label[contains(.,'Ville')]]/span//text()").extract_first()
        if address:
            item_loader.add_value("address",address.strip())    
            item_loader.add_value("city",address.strip())
        else:
            address = response.xpath("//span[@class='info']/text()").extract() 
            if address:
                item_loader.add_value("address", address[-3])
                item_loader.add_value('city', address[-3].split()[-1])
        rent =response.xpath("//div[@class='roominfo']//text()").extract_first()
        if rent:
            rent = rent.split("€")[0].strip().split(" ")[-1].replace(",",".")
            item_loader.add_value("rent", int(float(rent)))
            item_loader.add_value("currency", "EUR")
      
        floor = response.xpath("//div[label[contains(.,'Nombre étages')]]/span//text()").extract_first() 
        if floor:   
            item_loader.add_value("floor",floor.strip())      
        room_count = response.xpath("//div[label[.='Chambres']]/span//text()").extract_first() 
        if room_count:   
            item_loader.add_value("room_count",room_count.strip())
        zipcode = response.xpath("//div[label[contains(.,'Code postal')]]/span//text()").extract_first() 
        if zipcode:   
            item_loader.add_value("zipcode",zipcode.strip())
        bathroom_count = response.xpath("//div[label[contains(.,'Salle de bain')]]/span//text()").extract_first() 
        if bathroom_count:   
            item_loader.add_value("bathroom_count",bathroom_count.strip())
        square =response.xpath("//div[label[.='Surface']]/span//text()").extract_first()
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters) 
        
        utilities =response.xpath("//div[label[contains(.,'Honoraires état des lieux locataire')]]/span//text()").extract_first()    
        if utilities:
            item_loader.add_value("utilities",utilities.replace(" ",""))  

        furnished =response.xpath("//div[label[contains(.,'Meublé')]]/span//text()").extract_first()    
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        elevator =response.xpath("//div[label[contains(.,'Ascenseur')]]/span//text()").extract_first()    
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
        parking =" ".join(response.xpath("//div[label[contains(.,'parking') or contains(.,'garage')]]/span//text()").extract())
        if parking:
            if "non" in parking.lower() or "1" not in parking:
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        balcony =response.xpath("//div[label[contains(.,'balcon')]]/span//text()").extract_first()    
        if balcony:
            if "non" in balcony.lower()  or "0" in balcony:
                item_loader.add_value("balcony", False)
            else:
                item_loader.add_value("balcony", True)
        terrace =response.xpath("//div[label[contains(.,'terrasse')]]/span//text()").extract_first()    
        if terrace:
            if "non" in terrace.lower()  or "0" in terrace:
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)
        deposit =response.xpath("//div[label[contains(.,'de garantie')]]/span//text()").extract_first()    
        if deposit:
            item_loader.add_value("deposit",deposit.replace(" ","")) 
        desc = " ".join(response.xpath("//div[@id='description']/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        energy = response.xpath("//div[label[contains(.,'Conso Energ')]]/span//text()[not(contains(.,'Vierge'))]").extract_first() 
        if energy:   
            item_loader.add_value("energy_label",energy.strip())
        
        name = response.xpath("//div[@class='mtitle']/text()").get()
        item_loader.add_value("landlord_name", name)
        item_loader.add_xpath("landlord_phone", "//div[h3[contains(.,'agence')]]//div[@class='member-contact']//a[contains(@href,'tel')]//@title")
        item_loader.add_value("landlord_email", "mdelbourg@stephaneplazaimmobilier.com")
        
        prop_id = response.xpath("//app-product-share/@id").get()
        if prop_id:
            photo_url = "https://mantes.stephaneplazaimmobilier.com/product/media/" + prop_id
            yield Request(photo_url, callback=self.get_images, meta={"item_loader":item_loader})
        else: yield item_loader.load_item()

    def get_images(self, response):

        item_loader = response.meta["item_loader"]
        data = json.loads(response.body)
        images = []
        for item in data["base"]: images.append(item["fullurl"])
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        yield item_loader.load_item()
        