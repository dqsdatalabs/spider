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
    name = 'nicecentreport_stephaneplazaimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
           
            {"url": "https://nicecentreport.stephaneplazaimmobilier.com/location/appartement", "property_type": "apartment"},
	        {"url": "https://nicecentreport.stephaneplazaimmobilier.com/location/maison", "property_type": "house"},
        ] 
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    def jump(self, response):

        cities = json.loads(response.body)
        apt_url = "https://nicecentreport.stephaneplazaimmobilier.com/search/rent?target=rent&type[]=1&agency_id=219&idagency=151890"
        hs_url = "https://nicecentreport.stephaneplazaimmobilier.com/search/rent?target=rent&type[]=2&agency_id=219&idagency=151890"

        for city in cities:
            apt_url += "&location[]=" + str(city["id_polygone"])
            hs_url += "&location[]=" + str(city["id_polygone"])

        yield Request(apt_url, callback=self.parse, meta={'property_type': "apartment"})     
        yield Request(hs_url, callback=self.parse, meta={'property_type': "house"}) 

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
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//li[contains(.,'Référence')]/text()", input_type="F_XPATH", split_list={"rence":1})
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Nicecentreport_Stephaneplazaimmobilier_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//label[contains(.,'Code postal')]/following-sibling::span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//label[contains(.,'Ville')]/following-sibling::span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@id='description']/p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//label[.='Surface']/following-sibling::span/text()", input_type="F_XPATH", get_num=True, split_list={".":0, "m":0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//label[contains(.,\"Salle de bain\")]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='roominfo'][contains(.,'Loyer ')]/text()", input_type="F_XPATH", get_num=True, split_list={",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//label[contains(.,'Depot de garantie')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//label[contains(.,'Charges recuperable')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//label[contains(.,'Meublé')]/following-sibling::span/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//label[contains(.,'balcon')]/following-sibling::span/text()[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//label[contains(.,'terras')]/following-sibling::span/text()[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//label[contains(.,'Date')]/following-sibling::span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//div[@class='member-contact']/a/@title", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="//div[@class='member-contact']/app-obfuscate-email/@email", input_type="F_XPATH")
        landlord_name = response.xpath("//div[@class='mtitle']/text()[normalize-space()]").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())
        else:
            item_loader.add_xpath("landlord_name","//div[@id='agency-title']/a/text()")
       
        room_count = response.xpath("//label[.='Chambres']/following-sibling::span/text()[1]").get()
        if room_count and room_count.strip() !='0':
            item_loader.add_value("room_count", room_count.strip())
        else:
            item_loader.add_xpath("room_count","//label[.='Nombre pièces']/following-sibling::span/text()[1]")
        

        country = response.xpath("//label[contains(.,'Pays')]/following-sibling::span/text()").get()
        city = response.xpath("//label[contains(.,'Ville')]/following-sibling::span/text()").get()
        zipcode = response.xpath("//label[contains(.,'Code postal')]/following-sibling::span/text()").get()
        address = ""
        if city: address += city.strip() + " "
        if zipcode: address += zipcode.strip() + " "
        if country: address += country.strip() + " "
        if address: item_loader.add_value("address", address.strip())

        parking = response.xpath("//label[contains(.,'Nombre places parking') or contains(.,'garage')]/following-sibling::span/text()[.!='0']").get()
        if parking:
            item_loader.add_value("parking", True)

        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//label[contains(.,'Conso Energ')]/following-sibling::span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor", input_value="//label[contains(.,'Etage X/X')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True, split_list={"/":0})
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//label[contains(.,'Ascenseur')]/following-sibling::span/text()", input_type="F_XPATH", tf_item=True)

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