# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from  geopy.geocoders import Nominatim
from html.parser import HTMLParser

class MySpider(Spider):
    name = 'imogroup_leman_com'
    start_urls = ["https://www.imogroup-grandgeneve.com/fr/locations"] 
    execution_type='testing'
    country='france'
    locale='fr'
    thousand_separator = ',' 
    scale_separator = '.' #LEVEL-1
    external_source='Imogroupleman_PySpider_france_fr'
    custom_settings = {
      "PROXY_ON": True,    
    }
    headers = {
        'content-type': "multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW",
        'cache-control': "no-cache",
    }

    # 1. FOLLOWING
    def parse(self, response):
        # page = response.meta.get("page",2)
        
        # seen = False
        for item in response.xpath("//div[@class='vignette-mf']"):
            f_url = response.urljoin(item.xpath(".//a//@href").get())
            prop_type = item.xpath(".//div//h3//text()[1]").get()
            if "Appartement" in prop_type:
                prop_type = "apartment"
            elif "Maison" in prop_type:
                prop_type = "house"
            else:
                prop_type = None
            
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : prop_type},
            )
        #     seen = True
        
        # if page == 2 or seen:
        #     url = f"https://www.imogroup-leman.com/fr/locations/{page}"
        #     payload = "------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"LocationSearch[commune]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"LocationSearch[loyer_min]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"LocationSearch[loyer_max]\"\r\n\r\n1000000000\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"LocationSearch[rayonCommune]\"\r\n\r\n10\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"LocationSearch[tri]\"\r\n\r\nloyerCcTtcMensuel|asc\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data; name=\"LocationSearch[noMandat]\"\r\n\r\n\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW--"
        #     yield Request(
        #         url = url,
        #         body = payload,
        #         method = "POST",
        #         callback = self.parse,
        #         headers = self.headers,
        #         meta = {"page" : page+1}
        #     )
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", self.external_source)
        external_id=response.xpath("//span[@class='reference']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1].strip())

        title=response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
            
        latitude_longitude = response.xpath("//script[contains(.,'L.map')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('position = [')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('position = [')[1].split(',')[1].split(']')[0].strip()
            geolocator = Nominatim(user_agent=response.url)
            try:
                location = geolocator.reverse(latitude + ', ' + longitude, timeout=None)
                if location.address:
                    address = location.address
                    if location.raw['address']['postcode']:
                        zipcode = location.raw['address']['postcode']
            except:
                address = None
                zipcode = None
            if address:
                item_loader.add_value("address", address)
                item_loader.add_value("zipcode", zipcode)
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)
        city=response.xpath("//span[@class='commune']/text()").get()
        if city:
            item_loader.add_value("city",city)
        square_meters = response.xpath("//span[contains(.,'Surface habitable')]/following-sibling::b/text()").get()
        if square_meters:
            square_meters = square_meters.split('m')[0].strip()
            item_loader.add_value("square_meters", square_meters)
        description = response.xpath("//div[@id='description']/div/text()").getall()    
        if description:
            item_loader.add_value("description", description)

        utilities=response.xpath("//div[@class='charges']//text()").getall() 
        if utilities:
            for i in utilities:
                if "charges" in i:
                    utilities=i.split("Dont")[-1].split("€")[0].replace("-","").replace(":","").strip()
                    if utilities:
                        item_loader.add_value("utilities",utilities)
                if "Dépôt" in i:
                    deposit=i.split(":")[-1].split("€")[0].replace(" ","").strip()
                    if deposit:
                        item_loader.add_value("deposit",deposit)


        room_count = response.xpath("//span[.='f']/parent::td/text()").get()
        if room_count:
            room_count = room_count.split(':')[1].strip()
            item_loader.add_value("room_count", room_count)
        elif "pièce" in title:
            room=title.split("pièce")[0].strip().split(" ")[-1]
            item_loader.add_value("room_count", room)
        
        bathroom_count=response.xpath("//span[contains(.,'Salle d')]/following-sibling::b/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        parking=response.xpath("//span[contains(.,'Garage')]/following-sibling::b/text()").get()
        if parking:
            item_loader.add_value("parking",True)
            
        rent = response.xpath("//span[@class='prix has_sup']/text()").get()
        if rent:
            rent = rent.split('€')[0].split(":")[1].strip().replace(' ', '')
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'EUR')
           

        images = [x for x in response.xpath("//a[@class='item photo']//@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        a = response.xpath("//table[@id='tableau_conso']//tr[1]//div/text()").get()
        b = response.xpath("//table[@id='tableau_conso']//tr[2]//div/text()").get()
        c = response.xpath("//table[@id='tableau_conso']//tr[3]//div/text()").get()
        d = response.xpath("//table[@id='tableau_conso']//tr[4]//div/text()").get()
        e = response.xpath("//table[@id='tableau_conso']//tr[5]//div/text()").get()
        f = response.xpath("//table[@id='tableau_conso']//tr[6]//div/text()").get()
        g = response.xpath("//table[@id='tableau_conso']//tr[7]//div/text()").get()
        energy_label = ''
        if a and a.strip() != '':
            energy_label = 'A'
        elif b and b.strip() != '':
            energy_label = 'B'
        elif c and c.strip() != '':
            energy_label = 'C'
        elif d and d.strip() != '':
            energy_label = 'D'
        elif e and e.strip() != '':
            energy_label = 'E'
        elif f and f.strip() != '':
            energy_label = 'F'
        elif g and g.strip() != '':
            energy_label = 'G'
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        floor = response.xpath("//td[contains(.,'Étage')]/text()").get()
        if floor:
            floor = floor.split(':')[1].strip()
            item_loader.add_value("floor", floor)

        item_loader.add_value("landlord_name", "IMOGROUP Léman")
        
        landlord_phone = response.xpath("//span[@class='telAgenceBien']/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.split(':')[1].strip()
            item_loader.add_value("landlord_phone", landlord_phone)
        
        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data