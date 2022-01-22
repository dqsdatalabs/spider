# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
from html.parser import HTMLParser
import json
import re

class MySpider(Spider):
    name = 'bcnadvisors_com'
    execution_type='testing'
    country='spain'
    locale='en'

    def start_requests(self):
        start_urls = [
            {"url": "https://www.bcn-advisors.com/alquiler-aticos/","property_type": "house"},
            {"url": "https://www.bcn-advisors.com/alquiler-casas-lujo","property_type": "house"},
            {"url": "https://www.bcn-advisors.com/alquiler-lofts","property_type": "house"},
            {"url": "https://www.bcn-advisors.com/alquiler-pisos-lujo/","property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})
    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[contains(@class,'propiedad-listado')]/a[@class='button-seemore']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        pagination = response.xpath("//div[@class='pagination']/a[contains(@class,'next')]/@href").get()
        if pagination:
            url = response.urljoin(pagination)
            yield Request(url, callback=self.parse, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        rented = response.xpath("//span[@class='flag']/text()[.='Alquilado']")
        if rented:
            return
        title = response.xpath("//h1/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Bcnadvisors_PySpider_"+ self.country + "_" + self.locale)

        address = "".join(response.xpath("normalize-space(//li[strong[contains(.,'Zona')]]/text()[last()])").getall())
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address.strip()))
            item_loader.add_value("city", re.sub("\s{2,}", " ", address.strip()))
                
        square_meters = "".join(response.xpath("substring-before(//li[strong[.='Superficie interior:']]/text(),'m2')").getall())
        if square_meters:
            item_loader.add_value("square_meters", square_meters)
        else:
            item_loader.add_xpath("square_meters", "substring-before(normalize-space(//li[strong[contains(.,'Interior:')]]/span/text()),'m²')")

        room_count = "".join(response.xpath("normalize-space(//li[strong[contains(.,'Dormitorios')]]/span/text())").extract())
        if room_count:
            item_loader.add_value("room_count", room_count.strip())


        bathroom_count = response.xpath("//li[strong[.='Baños:']]/text()").get()
        if bathroom_count:            
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        # rent = "".join(response.xpath("//li[strong[.='Precio:']]/text()").extract())
        rent = "".join(response.xpath("//div[contains(@class,'precio--int')]/text()").extract())
        if rent:
            rent = rent.strip().replace('.', '')
            item_loader.add_value("rent_string", rent+"€")

        external_id = response.xpath("//li[strong[.='Referencia:']]/text()").get()
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)

        description = "".join(response.xpath("//div[@class='property-description']//p/text()").getall())    
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description)

            if "balcones" in description:
                item_loader.add_value("balcony", True)
            if "lavadora" in description:
                item_loader.add_value("washing_machine", True)
            if "lavavajillas" in description:
                item_loader.add_value("dishwasher", True)
           
        images = [x for x in response.xpath("//div[@id='sync1']//img/@src[not(contains(.,'data:image/svg+xml,%3Csvg%20xmlns='))]").getall()]
        if images:
            item_loader.add_value("images", list(set(images)))
            item_loader.add_value("external_images_count", str(len(images)))

        energy_label = "".join(response.xpath("normalize-space(//ul[@class='property-bar-features']/li/strong[contains(.,'Calificación:')]/following-sibling::span/text()[not(contains(.,'T'))])").getall())
        if energy_label:
            energy_label = energy_label.strip()
            item_loader.add_value("energy_label", energy_label)

        furnished = "".join(response.xpath("//li[strong[contains(.,'Amueblado')]]/text()").getall()).strip()
        
        if furnished:
            if "sí" in furnished.strip().lower():
                item_loader.add_value("furnished", True)
            elif 'no' in furnished.strip().lower():
                item_loader.add_value("furnished", False)
            

        parking = "".join(response.xpath("//ul[@class='features-ul'][2]/li[contains(.,'Parking')]/text()").getall())
        if parking:
            item_loader.add_value("parking", True)
        else:
            park = "".join(response.xpath("//li[strong[.='Parking:']]/text()[.!='0']").extract()).strip()
            if park:
                if park != "0":
                    item_loader.add_value("parking", True)
                else:
                    item_loader.add_value("parking", False)

        elevator = "".join(response.xpath("//ul[@class='features-ul'][2]/li[contains(.,'Ascensor')]/text()").getall())
        if elevator:
            elevator = True
            item_loader.add_value("elevator", elevator)

        terrace = "".join(response.xpath("//ul[@class='features-ul'][2]/li[contains(.,'Terraza')]/text()").getall())
        if terrace:
            item_loader.add_value("terrace", True)
        else:
            terrace = "".join(response.xpath("substring-before(//li[strong[.='Superficie Terraza:']]/text(),' m2')").extract()).strip()
            if terrace:
                if terrace != "0":
                    item_loader.add_value("terrace", True)
                else:
                    item_loader.add_value("terrace", False)

        swimming_pool = response.xpath("//li[contains(.,'Pool')]").get()
        if swimming_pool:
            swimming_pool = True
            item_loader.add_value("swimming_pool", swimming_pool)

        landlord_phone = response.xpath("//ul[@class='contacts-footer']/li[3]//a/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)

        landlord_email = response.xpath("//ul[@class='contacts-footer']/li[2]//a/text()").get()
        if landlord_email:
            landlord_email = landlord_email.strip()
            item_loader.add_value("landlord_email", landlord_email)
        item_loader.add_value("landlord_name", "Bcn Advisors")
                
        lat_lng = response.xpath("//script[contains(.,'center:') and contains(.,'lng')]//text()").get()
        if lat_lng:
            lat = lat_lng.split("lat:")[1].split(",")[0].strip()
            lng = lat_lng.split("lng:")[1].split("}")[0].strip()
            if lat and lng:
                if lat.strip()!="0" and lng.strip()!="0":
                    
                    item_loader.add_value("latitude", lat)
                    item_loader.add_value("longitude", lng)

        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data