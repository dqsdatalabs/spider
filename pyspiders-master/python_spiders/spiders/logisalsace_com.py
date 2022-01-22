# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from html.parser import HTMLParser
import dateparser

class MySpider(Spider):
    name = 'logisalsace_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {"url": "http://www.logis-alsace.com/recherche?a=2&b%5B%5D=house&c=&radius=0&d=0&e=illimit%C3%A9&f=0&x=illimit%C3%A9&do_search=Rechercher", "property_type": "house"},
            {"url": "http://www.logis-alsace.com/recherche?a=2&b%5B%5D=appt&c=&radius=0&d=0&e=illimit%C3%A9&f=0&x=illimit%C3%A9&do_search=Rechercher", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse,
                             meta={'property_type': url.get('property_type')})
    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='details']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "LogisalsacePySpider_"+ self.country + "_" + self.locale)

        rented = response.xpath("//div[@class='band_rotate']/text()").extract_first()
        if rented :
            return
        title = response.xpath("//div[@id='page_title']/h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        else:
            title =" ".join(response.xpath("//td[@itemprop='name']//text()").getall())
            if title:
                item_loader.add_value("title", title.strip())
        item_loader.add_value("external_link", response.url)

        external_id = response.xpath("//td[.='Référence']/parent::tr/td[2]/text()").get()
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)

        latitude_longtitude = response.xpath("//div[@id='mini_map_container']/following-sibling::script[1]/text()").get()
        if latitude_longtitude:
            latitude_longtitude = latitude_longtitude.split('[')[1].split(']')[0]
            latitude = latitude_longtitude.split(',')[0].strip()
            longtitude = latitude_longtitude.split(',')[1].strip()
            item_loader.add_value("longitude", longtitude)
            item_loader.add_value("latitude", latitude)

        address = "".join(response.xpath("//div[@class='tech_detail']//tr[./td[.='Ville']]/td[2]//text()").extract())
        if address:
            item_loader.add_value("address", address)
            zipcode = address.split(" ")[-1]
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("city", address.split(zipcode)[0].strip())
            

        item_loader.add_value("property_type", response.meta.get('property_type'))

        square_meters = response.xpath("//td[.='Surface']/parent::tr/td[2]/text()").get()
        if square_meters:
            if len(square_meters.split(' ')) > 1:
                square_meters = square_meters.strip().split(' ')[0]
            elif len(square_meters.split('.')) > 1:
                square_meters = square_meters.strip().split('.')[0]
            square_meters = square_meters.strip()
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//td[.='Chambres']/parent::tr/td[2]/text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)

        bathroom=response.xpath("//td[.='Salle de bains' or contains(.,'Salle d')]/parent::tr/td[2]/text()").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom)
        
        price = response.xpath("//td[@itemprop='price']/span/text()").get()
        if price:
            item_loader.add_value("rent_string", price.replace(" ",""))
        
        
        
        description = response.xpath("//div[@id='details']//text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)
        date=response.xpath("//td[.='Disponibilité']/parent::tr/td[2]/text()").get()
        if date:            
            date_parsed = dateparser.parse(date, languages=['fr'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
       
        images = [x for x in response.xpath("//div[@id='layerslider']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        deposit = response.xpath("//div[@class='basic_copro']//text()[contains(.,' de garantie')]").get()
        if deposit:
            deposit = deposit.split(" de garantie")[1].split("€")[0].strip()
            item_loader.add_value("deposit", deposit)
        utilities=response.xpath("//td[.='Charges']/parent::tr/td[2]/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(" ")[0])
        elif not utilities:
            utilities = response.xpath("//div[@class='basic_copro']//text()[contains(.,'charges')]").get()
            if utilities:
                utilities = utilities.split("charges")[1].split("€")[0].strip()
                item_loader.add_value("utilities", utilities)
        floor = response.xpath("//td[.='Étage']/parent::tr/td[2]/text()").get()
        if floor:
            floor = floor.strip()
            item_loader.add_value("floor", floor)

        furnished = response.xpath("//td[.='Ameublement']/parent::tr/td[2]/text()").get()
        if furnished:
            if furnished.lower().startswith("non"):
                furnished = False
            else:
                furnished = True
            item_loader.add_value("furnished", furnished)
        
        swimming_pool = response.xpath("//td[.='Piscine']/parent::tr/td[2]/text()").get()
        if swimming_pool and "non" in swimming_pool.lower():
            item_loader.add_value("swimming_pool", False)
        elif swimming_pool: 
            item_loader.add_value("swimming_pool", True)
        parking = response.xpath("//div[contains(@class,'map_caterogy')]/label[contains(.,'Parking')]//text()").get()
        if parking:    
            item_loader.add_value("parking", True)
        elevator = response.xpath("//td[.='Ascenseur']/parent::tr/td[2]/text()").get()
        if elevator:
            if elevator.lower().startswith("non"):
                elevator = False
            else:
                elevator = True
            item_loader.add_value("elevator", elevator)
    
        energy_label = response.xpath("//div[@class='dpe_container']//div[@class='dpe-letter']/b/text()").get()
        if energy_label:
            energy_label = energy_label.split(':')[0].strip()
            item_loader.add_value("energy_label", energy_label)

        item_loader.add_value("landlord_phone", "03 89 08 88 99")
        item_loader.add_value("landlord_name", "Logis d'Alsace")
        item_loader.add_value("landlord_email", "altkirch@logis-alsace.com")
         

        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data