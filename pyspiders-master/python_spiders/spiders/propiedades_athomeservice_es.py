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
from urllib.parse import urljoin

class MySpider(Spider):
    name = 'propiedades_athomeservice_es'
    execution_type='testing'
    country='spain'
    locale='es'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://propiedades.athomeservice.es/es/browser/s1?quick_search%5Btof%5D=2&quick_search%5BpropertyTypes%5D=%7B%22g1%22%3A%7B%22tp%22%3A%5B1%2C2%2C4%2C8%2C16%5D%2C%22tpe%22%3A%5B%5D%2C%22c%22%3Atrue%7D%2C%22g2%22%3A%7B%22tp%22%3A%5B%5D%2C%22tpe%22%3A%5B%5D%2C%22c%22%3Afalse%7D%2C%22g4%22%3A%7B%22tp%22%3A%5B%5D%2C%22tpe%22%3A%5B%5D%2C%22c%22%3Afalse%7D%2C%22g8%22%3A%7B%22tp%22%3A%5B%5D%2C%22tpe%22%3A%5B%5D%2C%22c%22%3Afalse%7D%2C%22g16%22%3A%7B%22tp%22%3A%5B%5D%2C%22tpe%22%3A%5B%5D%2C%22c%22%3Afalse%7D%2C%22text%22%3A%22Pisos%22%7D&quick_search%5BfullLoc%5D=%7B%22p%22%3A%5B%5D%2C%22c%22%3A%5B%5D%2C%22d%22%3A%5B%5D%2C%22q%22%3A%5B%5D%2C%22text%22%3A%22%22%7D&quick_search%5Beti%5D=&quick_search%5B_token%5D=s9nzsD8GrarWCG98Wqf21hU1jyvCiieGyIZGR7KqEx4",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "http://propiedades.athomeservice.es/es/browser/s1?quick_search%5Btof%5D=2&quick_search%5BpropertyTypes%5D=%7B%22g1%22%3A%7B%22tp%22%3A%5B%5D%2C%22tpe%22%3A%5B%5D%2C%22c%22%3Afalse%7D%2C%22g2%22%3A%7B%22tp%22%3A%5B32%2C64%2C128%2C256%2C16384%5D%2C%22tpe%22%3A%5B%5D%2C%22c%22%3Atrue%7D%2C%22g4%22%3A%7B%22tp%22%3A%5B%5D%2C%22tpe%22%3A%5B%5D%2C%22c%22%3Afalse%7D%2C%22g8%22%3A%7B%22tp%22%3A%5B%5D%2C%22tpe%22%3A%5B%5D%2C%22c%22%3Afalse%7D%2C%22g16%22%3A%7B%22tp%22%3A%5B%5D%2C%22tpe%22%3A%5B%5D%2C%22c%22%3Afalse%7D%2C%22text%22%3A%22Casas%22%7D&quick_search%5BfullLoc%5D=%7B%22p%22%3A%5B%5D%2C%22c%22%3A%5B%5D%2C%22d%22%3A%5B%5D%2C%22q%22%3A%5B%5D%2C%22text%22%3A%22%22%7D&quick_search%5Beti%5D=&quick_search%5B_token%5D=s9nzsD8GrarWCG98Wqf21hU1jyvCiieGyIZGR7KqEx4",

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

       
        for item in response.xpath("//div[@class='property-image']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//i[@class='fa fa-chevron-right']/../@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )
            
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title","//div//h2[@class='detail-title']//span/text()")
        item_loader.add_value("external_source", "Propiedadesathomeservice_PySpider_"+ self.country + "_" + self.locale)

        address = "".join(response.xpath("//div[@class='panel-body']//div[contains(@style,'padding:')]/text()").extract())
        if address:
            item_loader.add_value("address",address.strip())

        zipcode = "".join(response.xpath("//div[@class='panel-body']//div[contains(@style,'padding:')]/br/following-sibling::text()").getall())
        if zipcode:
            zipcode = zipcode.split(',')[0].strip()
            if zipcode != "" and zipcode.isnumeric():
                item_loader.add_value("zipcode", zipcode)

        latitude_longitude = response.xpath("//script[contains(.,'mymap')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('setView([')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('setView([')[1].split(',')[1].split(']')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
            
        square_meters = response.xpath("//div[contains(.,'Constr')]/i/parent::div/text()[2]").get()
        if square_meters:
            square_meters = square_meters.split(':')[1].split('m')[0].strip()
            item_loader.add_value("square_meters", square_meters)
        description = "".join(response.xpath("//div[@class='panel-default pgl-panel property']/div[2]/p/text()").getall())    
        if description:
            item_loader.add_value("description", description)
            if "garage" in description:
                item_loader.add_value("parking",True)
            if "terraza" in description:
                item_loader.add_value("terrace",True)

        room_count = response.xpath("//div[contains(.,'Dormitorio')]/i/parent::div/text()[2]").get()
        if room_count:
            room_count = room_count.strip().split(' ')[0]
            item_loader.add_value("room_count", room_count)
        elif not room_count and description:
            desc_value=""
            if "dormitorios" in description:
                desc_value= "dormitorios"
            elif "dormiorios" in description:
                desc_value= "dormiorios"
            if desc_value:
                room = description.split(desc_value)[0].strip().split(" ")[-1]
                room_count = textToNumber(room)
                if room_count!=0:
                    item_loader.add_value("room_count", room_count)
            
        bathroom_count = response.xpath("//div[contains(.,'Baños')]/i/parent::div/text()[2]").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(' ')[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        rent = response.xpath("//h3[@class='detail-title-price']/text()").get()
        if rent:
            # rent = rent.split('€')[0].strip().replace('.', '')
            item_loader.add_value("rent_string", rent)
            # item_loader.add_value("currency", "EUR")

        external_id = response.xpath("//h3[@class='ref-detail']/text()[2]").get()
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)

        

        
        city = response.xpath("//div[@id='collapseThree']//div[@class='col-md-6 col-sm-6 col-xs-12']/text()").getall()
        if city:
            fulladd = ''
            for c in city:
                fulladd += c.strip() + ' '
            city = fulladd.split('(')[1].split(')')[0]
            item_loader.add_value("city", city)
        
        images = [urljoin('https://athomeservice.inmoenter.com', x) for x in response.xpath("//div[@id='slider']//ul[@class='slides']//img/@src").getall()]
        if images:
            item_loader.add_value("images", list(set(images)))
            # item_loader.add_value("external_images_count", str(len(images)))

        furnished = response.xpath("//div[contains(.,'Amueblado')]/i/parent::div").get()
        if furnished:
            furnished = True
            item_loader.add_value("furnished", furnished)

        parking = response.xpath("//div[contains(.,'Garaje')]/i/parent::div").get()
        if parking:
            parking = True
            item_loader.add_value("parking", parking)

        elevator = response.xpath("//div[contains(.,'Ascensor')]/i/parent::div").get()
        if elevator:
            elevator = True
            item_loader.add_value("elevator", elevator)

        terrace = response.xpath("//div[contains(.,'Terraza')]/i/parent::div").get()
        if terrace:
            terrace = True
            item_loader.add_value("terrace", terrace)
        
        washing_machine = response.xpath("//div[contains(.,'Lavadora')]/i/parent::div").get()
        if washing_machine:
            washing_machine = True
            item_loader.add_value("washing_machine", washing_machine)

        swimming_pool = response.xpath("//div[contains(.,'Piscina')]/i/parent::div").get()
        if swimming_pool:
            swimming_pool = True
            item_loader.add_value("swimming_pool", swimming_pool)

        landlord_phone = response.xpath("//address//a[contains(@href,'tel')][1]/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip().replace('\xa0', '')
            item_loader.add_value("landlord_phone", landlord_phone)

        landlord_email = response.xpath("//address//a[contains(@href,'mail')]/text()").get()
        if landlord_email:
            landlord_email = landlord_email.strip()
            item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_name", "AT HOME SERVICE")
        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data
       
def textToNumber(z):                                                    
    if(z == "uno"):                                                 
        return 1                                                    
    elif(z == "dos"):                                                   
        return 2                                                    
    elif(z == "tres"):                                                   
        return 3                                               
    elif(z == "cuatro"):                                                   
        return 4                                                  
    elif(z == "cinco"):                                                   
        return 5                                                   
    elif(z == "seis"):                                                   
        return 6                                                  
    elif(z == "siete"):                                                   
        return 7                                                   
    else:
        return 0
        
        
          

        

      
     