# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.http import headers
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re 
class MySpider(Spider):
    name = 'pisacasa_com'
    execution_type='testing'
    country='italy'
    locale='it'
    external_source = "Pisacasa_PySpider_italy"
    start_urls = ['https://pisacasa.com/wp-json/myhome/v1/estates?currency=any']  # LEVEL 1
    custom_settings = {
        "PROXY_ON" : True,
        # "CONCURRENT_REQUESTS": 3,
        # "COOKIES_ENABLED": False,
        "RETRY_TIMES": 3,
        # "DOWNLOAD_DELAY": 5,

    } 
    download_timeout = 200
    
    formdata = {
        "data[tipo-di-offerta][compare]": "=",
        "data[tipo-di-offerta][key]": "tipo-di-offerta",
        "data[tipo-di-offerta][slug]": "tipo-di-offerta",
        "data[tipo-di-offerta][values][0][name]": "In affitto",
        "data[tipo-di-offerta][values][0][value]": "affitto",
        "page": "1",
        "limit": "12",
        "sortBy": "newest",
        "currency": "any",
    }
    
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "en,tr-TR;q=0.9,tr;q=0.8,en-US;q=0.7",
        "origin": "https://pisacasa.com",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36",
    }
     
    def start_requests(self):
        yield FormRequest(
            url=self.start_urls[0],
            method='POST',
            formdata=self.formdata,
            headers=self.headers,
            callback=self.parse
        )
    
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False

        data = json.loads(response.body)
        for item in data["results"]:
            follow_url = response.urljoin(item["link"])
            yield Request(follow_url, callback=self.populate_item)

            seen = True

        if page == 2 or seen:
            self.formdata["page"] = str(page)
            yield FormRequest(
                self.start_urls[0],
                dont_filter=True,
                formdata=self.formdata,
                callback=self.parse,
                meta={
                    "page": page+1,
                }
            )
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        prop_type = response.xpath("//ul[@class='mh-estate__list__inner']/li/strong[contains(.,'Tipologia ')]/following-sibling::a/@title").get()
        if get_p_type_string(prop_type):
            item_loader.add_value("property_type", get_p_type_string(prop_type))
        else: return
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//div[@class='mh-layout']/h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        external_id = response.xpath("//ul[@class='mh-estate__list__inner']/li/strong[contains(.,'Riferimento')]/following-sibling::text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        rent = response.xpath("(//div[@class='mh-estate__details__price']/div/div/text()[contains(.,'€')])[1]").get()
        if rent:
            item_loader.add_value("rent", rent.split('€')[0].strip())

        item_loader.add_value("currency", "EUR")

        square_meters = response.xpath("//ul[@class='mh-estate__list__inner']/li/strong[contains(.,'MQ')]/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(' ')[0].strip())

        room_count = response.xpath("//ul[@class='mh-estate__list__inner']/li/strong[contains(.,'Locali')]/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//ul[@class='mh-estate__list__inner']/li/strong[contains(.,'Bagni')]/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        floor = "".join(response.xpath("//ul[@class='mh-estate__list__inner']/li/strong[contains(.,'Piano')]/following-sibling::text()").get())
        if floor:
            floor=re.findall("\d+",floor)
            item_loader.add_value("floor", floor[0])

        city = response.xpath("//ul[@class='mh-estate__list__inner']/li/strong[contains(.,'Città')]/following-sibling::a/@title").get()
        if city:
            item_loader.add_value("city", city)

        uti = response.xpath("//ul[@class='mh-estate__list__inner']/li/strong[contains(.,'Spese')]/following-sibling::text()").get()
        if uti:
            utilities = re.findall(r'\d+', uti)
            if utilities:
                item_loader.add_value("utilities", utilities)

        address = response.xpath("//div[@class='small-text']/span/text()").get()
        if address:
            item_loader.add_value("address", address.split('via')[-1].strip())

        desc = "".join(response.xpath("//div[@class='mh-estate__section mh-estate__section--description']/p/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        energy_label = response.xpath("//ul[@class='mh-estate__list__inner']/li/strong[contains(.,'Energetica')]/following-sibling::text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())

        parking = response.xpath("//ul[@class='mh-estate__list__inner']/li/strong[contains(.,'Garage')]/following-sibling::text()").get()
        if parking:
            item_loader.add_value("parking", True)
        furnished = response.xpath("//ul[@class='mh-estate__list__inner']/li/text()[contains(.,'Arredato')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        balcony = response.xpath("//ul[@class='mh-estate__list__inner']/li/text()[contains(.,'Balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        elevator = response.xpath("//ul[@class='mh-estate__list__inner']/li/text()[contains(.,'Ascensore')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        images = [x for x in response.xpath("//div[contains(@class,'mh-popup-group')]/div[@class='swiper-slide']/a/@href").getall()]
        if images: 
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        latitude="".join(response.xpath("//estate-map[@id='myhome-estate-map']").extract())
        if latitude:
            item_loader.add_value("latitude",latitude.split("position")[-1].split("lat")[-1].split(",")[0].replace(":","").replace('"',""))
        longitude="".join(response.xpath("//estate-map[@id='myhome-estate-map']").extract())
        if longitude:
            item_loader.add_value("longitude",longitude.split("position")[-1].split("lng")[-1].split("}")[0].replace(":","").replace('"',""))

        landlord_name = response.xpath("//section[@class='mh-estate__agent']/div/h3/a/@title").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        landlord_phone = response.xpath("//div[@class='mh-estate__agent__phone']/a/span/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.split(':')[-1].strip())
        landlord_email = response.xpath("//div[@class='mh-estate__agent__email']/a/@href").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email.split(":")[-1])
        yield item_loader.load_item()
        
def get_p_type_string(p_type_string):
    if p_type_string and ("appartamento" in p_type_string.lower() or " local" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "loft" in p_type_string.lower() or "attico" in p_type_string.lower()):
        return "house"
    elif p_type_string and "stanza" in p_type_string.lower():
        return "room"
    else:
        return None