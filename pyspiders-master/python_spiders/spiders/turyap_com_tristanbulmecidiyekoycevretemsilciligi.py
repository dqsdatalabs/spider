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

class MySpider(Spider):
    name = 'turyap_com_tristanbulmecidiyekoycevretemsilciligi'
    execution_type='testing'
    country='turkey'
    locale='tr'

    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.turyap.com.tr/firma-ilanlari/istanbul-mecidiyekoy-cevre-temsilciligi/402861?st=2&mg=1&sg=1&fo=402861|402861&sort=6",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.turyap.com.tr/firma-ilanlari/istanbul-mecidiyekoy-cevre-temsilciligi/402861?st=2&mg=1&sg=8|10|13&fo=402861|402861&sort=6",
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@id='search_results']/div[contains(@class,'product-search')]"):
            follow_url = response.urljoin(item.xpath("./div[@class='wrapper']/a/@href").extract_first())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get("property_type")})

        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Turyapcomtristanbulmecidiyekoycevretemsilciligi_PySpider_"+ self.country + "_" + self.locale)

        item_loader.add_xpath("title", "//section/h1/text()")
        item_loader.add_value("property_type", response.meta.get("property_type"))
        #item_loader.add_css("", "")

        item_loader.add_value("external_link", response.url)
        
        address = " ".join(response.xpath("//h4//text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split('/')[0].strip())

        latitude_longitude = response.xpath("//script[contains(.,'$location_info')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('$location_info_lat=')[1].split(';')[0].strip()
            longitude = latitude_longitude.split('$location_info_lng=')[1].split(';')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        square_meters = response.xpath("//div[@class='productx col-xs-9 col-sm-6 col-md-8 col-lg-8']/span[4]/text()[2]").get()
        if square_meters:
            square_meters = square_meters.strip().split(' ')[0]
            item_loader.add_value("square_meters", square_meters)
        

        room_count = response.xpath("//span[contains(.,'Oda Sayısı') or contains(.,'Oda / Bölme')]/parent::div[@class='property_1']/span[2]/text()").get()
        saloon_count = response.xpath("//span[contains(.,'Salon Sayısı')]/parent::div/span[2]/text()").get()
        result = ''
        if room_count and saloon_count:
            result = str(int(room_count.strip()) + int(saloon_count.strip()))
        elif room_count:
            result = room_count.strip()
        if result:
            item_loader.add_value("room_count", result)

        item_loader.add_xpath("bathroom_count", "//div[span[. ='Banyo Sayısı']]/span[2]/text()")

        rent = response.xpath("//span[@class='oripri price']/text()").get()
        if rent:
            rent = rent.strip().split(' ')[0].replace(',', '')
            item_loader.add_value("rent", rent)
       

        currency = 'TRY'
        item_loader.add_value("currency", currency)

        external_id = response.xpath("//label[@class='text-info']/strong/text()").get()
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//div[@id='product_intro']//text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d + ' '
            filt = HTMLFilter()
            filt.feed(desc_html)
            res = filt.text
            if "Bulaşık Makinesi" in res:
                item_loader.add_value("dishwasher", True)
            if "mobilyalı" in res:
                item_loader.add_value("furnished", True)
            item_loader.add_value("description", res)

        

        images = [x for x in response.xpath("//div[@class='carousel-inner']//img/@src").getall()]
        if images:
            item_loader.add_value("images", list(set(images)))
            

        deposit = response.xpath("//span[contains(.,'Depozit')]/parent::div/span[2]/text()[.!='0 TL']").get()
        if deposit:
            deposit = deposit.strip().split(' ')[0]
            item_loader.add_value("deposit", deposit)

        utilities = "".join(response.xpath("//div[span[.='Aidat']]/span[2]/text()[.='0']").extract())
        if utilities:
            item_loader.add_value("utilities", utilities)

        floor = response.xpath("//span[contains(.,'Bulunduğu Kat')]/parent::div/span[2]/text()").get()
        if floor:
            floor = floor.strip()
            item_loader.add_value("floor", floor)

        elevator = response.xpath("//span[@class='property_name' and contains(.,'Asansör')]").get()
        if elevator:
            elevator = True
            item_loader.add_value("elevator", elevator)

        balcony = response.xpath("//span[@class='property_name' and contains(.,'Balkon')]").get()
        if balcony:
            balcony = True
            item_loader.add_value("balcony", balcony)
        parking = "".join(response.xpath("//div[@id='product_intro']//text()[contains(.,'otopark') or contains(.,'Otopark')]").extract())
        if parking:
            item_loader.add_value("parking", True)

        furnished = "".join(response.xpath("//h1/text()[contains(.,'Eşyalı')]").extract())
        if furnished:
            item_loader.add_value("furnished", True)
        elif "".join(response.xpath("//h1/text()[contains(.,'Eşyasız')]").extract()):
            item_loader.add_value("furnished", False)


        machine = "".join(response.xpath("//span[@class='property_name' and contains(.,'Beyaz Eşya')]").extract())
        if machine:
            item_loader.add_value("washing_machine", True)
            item_loader.add_value("dishwasher", True)
        swimming_pool = "".join(response.xpath("//span[@class='property_name' and contains(.,'Yüzme Havuzu')]").extract())
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        landlord_name = response.xpath("//div[@class='data-wrapper col-xs-12 col-sm-12 col-md-12 col-lg-12']/div[1]/text()").get()
        if landlord_name:
            landlord_name = landlord_name.strip()
            item_loader.add_value("landlord_name", landlord_name)

        landlord_phone = response.xpath("//div[@class='data-wrapper col-xs-12 col-sm-12 col-md-12 col-lg-12']/div[3]/a/span/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)
        
        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data