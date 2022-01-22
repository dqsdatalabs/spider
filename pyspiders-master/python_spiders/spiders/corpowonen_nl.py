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
import dateparser
import re

class MySpider(Spider): 
    name = 'corpowonen_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'  
    external_source='Corpowonen_PySpider_netherlands_nl'
    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.corpowonen.nl/aanbod/huur/heel-nederland/appartement/sorteer-adres-op/pagina-1/",
                "property_type" : "apartment"
            },
        ]# LEVEL 1
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='col-md-9']//div[@class='object-row']//span[@class='address']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//a[.='Volgende']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Corpowonen_PySpider_" + self.country + "_" + self.locale)

        title = response.xpath("//div[@class='address']/h2/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/object/")[-1].split("/")[0])

        latitude = response.xpath("//div[@id='map']/@data-latitude").get()
        longtitude = response.xpath("//div[@id='map']/@data-longitude").get()
        address_value ="".join(response.xpath("//div[@class='address']//text()[normalize-space()]").getall())
        if address_value:
            item_loader.add_value("address", address_value.strip())
        item_loader.add_value("longitude", longtitude)
        item_loader.add_value("latitude", latitude)
    
        item_loader.add_value("property_type", response.meta.get('property_type'))

        utilities = response.xpath("//td[contains(.,'Servicekosten')]/following-sibling::td/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split('€')[-1].split('p')[0].strip().replace(' ', ''))
        
        city_zipcode = response.xpath("//div[@class='address']/h4/text()").get()
        if city_zipcode:
            item_loader.add_value("city", city_zipcode.strip().split(' ')[-1].strip())

            city_zipcode=city_zipcode.strip().split(' ')[0]+city_zipcode.strip().split(' ')[1]
            item_loader.add_value("zipcode", city_zipcode)




        square_meters = response.xpath("//td[.='Woonoppervlakte']/parent::tr/td[2]/text()").get()
        if square_meters:
            square_meters = square_meters.split(' ')[0]
        else:
            square_meters_alternate = response.xpath("//td[.='Oppervlakte']/parent::tr/td[2]/text()").get()
            if square_meters_alternate:
                square_meters = square_meters_alternate.split(' ')[0]
        item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//td[.='Aantal kamers']/parent::tr/td[2]/text()").get()
        if room_count:
            if len(room_count.split('(')) > 1:
                room_count = room_count.split('(')[1].split(' ')[0].strip()
            else:
                room_count = room_count.replace("kamers","").strip()
            item_loader.add_value("room_count", room_count)

        price = response.xpath("//div[@class='price']/h2/text()").get()
        if price: 
            price = price.split(' ')[1]
            item_loader.add_value("rent", price)
            item_loader.add_value("currency", "EUR")

        description_1 = "".join(response.xpath("//div[@id='omschrijving']/text()").getall())
        if description_1:
            description = re.sub('\s{2,}', ' ', description_1.strip().replace("\n"," "))
            item_loader.add_value("description", str(description))
        if "zwembad" in description:
            item_loader.add_value("swimming_pool", True)
            
        images = [urljoin('https://www.corpowonen.nl', x) for x in response.xpath("//div[@class='swiper-wrapper']/div[@class='swiper-slide']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            # item_loader.add_value("external_images_count", str(len(images)))

        energy_label = response.xpath("//td[.='Energielabel']/parent::tr/td[2]/span/text()").get()
        if energy_label:
            energy_label = energy_label.strip()
            item_loader.add_value("energy_label", energy_label)   
      
        floor = response.xpath("//td[.='Woonlagen']/parent::tr/td[2]/text()").get()
        if floor:
            floor = floor.strip()
            item_loader.add_value("floor", floor)
        
        deposit = response.xpath("//td[.='Eenmalige kosten']/parent::tr/td[2]/text()").get()
        if deposit:
            if len(deposit.split(' ')) > 1:
                deposit = deposit.split(' ')[1].strip()
            else:
                deposit = deposit.strip()
            item_loader.add_value("deposit", deposit)

        item_loader.add_value("landlord_name", "CORPO Wonen")
        item_loader.add_value("landlord_phone", "085 401 10 20")
        item_loader.add_value("landlord_email", "info@corpowonen.nl")
        available_date = response.xpath("//td[.='Aangeboden sinds']/parent::tr/td[2]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        yield item_loader.load_item()