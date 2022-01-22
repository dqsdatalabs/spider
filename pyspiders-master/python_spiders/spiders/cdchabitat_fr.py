# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader 
import json 
from  geopy.geocoders import Nominatim 
from html.parser import HTMLParser
import math 
from scrapy.selector import Selector

class MySpider(Spider):
    name = 'cdchabitat_fr' 
    execution_type='testing'
    country='france'
    locale='fr' # LEVEL 1
    external_source="Cdchabitat_PySpider_france_fr"
    formdata={
        "display": "Galerie",
        "expanse": "",
        "cdTypage": "location",
        "cdCategorieFinancement": "",
        "notLLISocial": "",
        "notSocial": "",
        "fgConventionne": "",
        "fgLotStudefi": "",
        "order": "nb_loyer_total",
        "idSouhaitProspect": "",
        "pagerGo": "",
        "newSearch": "",
        "lbLieu": "",
        "cdTypeBien[]": "Appartement",
        "nbSurfaceHabitable": "",
        "nbLoyerTotal": "",
        "nbLoyerMin": "",
    }
    
    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.cdc-habitat.fr/Recherche/search",
            },

        ] # LEVEL 1

        for url in start_urls:
            yield FormRequest(url=url.get('url'),callback=self.parse,formdata=self.formdata)
    def parse(self, response):
        data=json.loads(response.body)['listeResultats']
        list=Selector(text=data).xpath("//div[@class='result-list-box-img']/a/@href").getall()
        for item in list:
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link",response.url)
        property_type = response.xpath("//div[@class='inner']//h1/text()").get()
        if property_type and "Maison" in property_type:
            item_loader.add_value("property_type", "house")
        elif property_type and ("Appartement" in property_type or "Résidence" in property_type):
            item_loader.add_value("property_type", "apartment")

        ref = response.xpath("//div[@class='ref']/text()").get()
        if ref:
            item_loader.add_value("external_id", ref.split(":")[1].strip())
 
        result = response.xpath("//div[@class='typage']/parent::h1/div[1]/text()").get()
        result2 = response.xpath("//div[@class='typage']/parent::h1/text()[2]").get()
        title = ''
        if result and result2:
            title += result.strip() + ' ' + result2.strip()
        else:
            result = response.xpath("//div[@class='inner']/h1/text()").get()
            if result:
                title = result.strip()
        item_loader.add_value("title", title)



       
        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude_longitude:
            try:
                latitude = latitude_longitude.split("latitude = ")[1].split(";")[0].replace("\'","").strip()
                longitude = latitude_longitude.split("longitude = ")[1].split(";")[0].replace("\'","").strip()
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)
            except:
                pass
        address_1 =  "".join(response.xpath("//div[@class='adresse']/text()").extract())
        address_2 =  "".join(response.xpath("//div[@class='ville']/text()").extract())  
        if address_1 or address_2:      
            item_loader.add_value("address", "{} {}".format(address_1.strip(),address_2.strip()))   
            if address_2:
                try:
                    zipcode =  address_2.split(" ")[0]
                    if zipcode.isdigit():
                        item_loader.add_value("zipcode",zipcode.strip())
                        item_loader.add_value("city", address_2.replace(zipcode,"").strip())
                except:
                    pass 
        else:
            address =  "".join(response.xpath("//div[@class='info_addr']/div[@class='data']//text()").extract())
            if address:
                item_loader.add_value("address",address.strip())
              
        square_meters = response.xpath("//div[@class='title' and contains(.,'urface')]/parent::div/div[2]/text()").get()
        if square_meters:
            square_meters = square_meters.strip().split(' ')[0]
            item_loader.add_value("square_meters", square_meters)
 
        room_count=item_loader.get_output_value("title")
        if room_count:
            room_count=room_count.split("pi")[0].strip().split(" ")[-1]
            if room_count:
                item_loader.add_value("room_count",room_count)

        rent = response.xpath("//div[@class='ensemble']/div[@class='prix']/text()[1]").get()
        if rent:
            rent = rent.split('€')[0].strip().replace(' ', '').replace(',', '.')
            rent = math.ceil(float(rent))
        else:
            rent = response.xpath("//p[@class='bandeau-price']/text()").get()
            if rent:
                rent = rent.split('€')[0].strip().replace(' ', '').replace(',', '.')
                rent = math.ceil(float(rent))
        if rent:
            item_loader.add_value("rent", str(rent))

        currency = 'EUR'
        item_loader.add_value("currency", currency)

        description="".join(response.xpath("//div[@class='intro-left']//p//text()").getall())
        if description:
            item_loader.add_value("description",description)

        images = [x for x in response.xpath("//div[@class='photos']//div[@class='swiper-wrapper slider_produit']//a/@href[not(contains(.,'javascript'))]").getall()]
        if images:
            item_loader.add_value("images", images)
            

        parking = response.xpath("//div[@class='title' and contains(.,'arking')]/parent::div/div[2]/text()").get()
        if parking:
            if parking.strip().lower() == 'oui':
                parking = True
            elif parking.strip().lower() == 'non':
                parking = False
        else:
            parking = response.xpath("//div[@class='plus']/span[contains(.,'arking')]").get()
            if parking:
                parking = True
        if parking:
            item_loader.add_value("parking", parking)

        elevator = response.xpath("//div[@class='title' and contains(.,'scenseur')]/parent::div/div[2]/text()").get()
        if elevator:
            if elevator.strip().lower() == 'oui':
                elevator = True
            elif elevator.strip().lower() == 'non':
                elevator = False
        else:
            elevator = response.xpath("//div[@class='plus']/span[contains(.,'scenseur')]").get()
            if elevator:
                elevator = True
        if elevator:
            item_loader.add_value("elevator", elevator)   

        balcony = response.xpath("//div[@class='plus']/span[contains(.,'alcon')]").get()
        if balcony:
            balcony = True
            item_loader.add_value("balcony", balcony)
 
        energy_label = response.xpath("//div[@class='diagnostique energetique']//img/@src[contains(.,'DPE_') and not(contains(.,'DPE_X'))]").get()
        if energy_label:
            try:
                energy = energy_label.split("DPE_")[1].split(".")[0].strip()
                if energy.isalpha():
                    item_loader.add_value("energy_label",energy)
            except:
                pass            

        item_loader.add_value("landlord_name", "Cdc Habitat")
        item_loader.add_value("landlord_phone", "09 70 40 25 09")

        yield item_loader.load_item()

# class HTMLFilter(HTMLParser):
#     text = ''
#     def handle_data(self, data):
#         self.text += data