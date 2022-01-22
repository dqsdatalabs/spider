# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'axel_immobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36",
    }
    def start_requests(self):
        
        start_urls = [
            {
                "url": "https://www.axel-immobilier.fr/ajax/ListeBien.php?TypeModeListeForm=text&tdp=5&filtre=2&lieu-alentour=0&langue=fr&MapWidth=100&MapHeight=0&DataConfig=JsConfig.Map.Liste&Pagination=0",
                "formdata" : {
                    "page": "",
                    "filtre": "2",
                },
                "type":"2",
                "property_type": "apartment",
            },
	        {
                "url": "https://www.axel-immobilier.fr/ajax/ListeBien.php?TypeModeListeForm=text&tdp=5&filtre=8&lieu-alentour=0&langue=fr&MapWidth=100&MapHeight=0&DataConfig=JsConfig.Map.Liste&Pagination=0",
                "formdata" : {
                    "page": "",
                    "filtre": "8",
                    },
                "type":"8",
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            self.headers["Referer"] = f"https://www.axel-immobilier.fr/fr/liste.htm?tdp=5&filtre={url.get('type')}"
            yield FormRequest(url=url.get('url'),
                            callback=self.parse,
                            formdata=url["formdata"],
                            headers=self.headers,
                            meta={'property_type': url.get('property_type'), 'formdata': url['formdata']})

    # 1. FOLLOWING
    def parse(self, response):
                
        for item in response.xpath("//div[contains(@class,'liste-bien-photo-frame')]//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type':response.meta.get('property_type')})
        
        last_page = response.xpath("//span[@class='nav-page-position']/text()").get()
        if last_page:
            last_page = last_page.split("/")[-1].strip()
            for i in range(1,int(last_page)+1):
                self.headers["Referer"] = f"https://www.axel-immobilier.fr/fr/liste.htm?page={i}&TypeModeListeForm=text&tdp=5&filtre=2&lieu-alentour=0"
                url = f"https://www.axel-immobilier.fr/ajax/ListeBien.php?page={i}&TypeModeListeForm=text&tdp=5&filtre=2&lieu-alentour=0&langue=fr&MapWidth=100&MapHeight=0&DataConfig=JsConfig.Map.Liste&Pagination=0"
                yield Request(url,
                    headers=self.headers,
                    callback=self.parse,
                    meta={"property_type":response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Axel_Immobilier_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h2[@class='detail-bien-ville']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h2[@class='detail-bien-ville']/text()", input_type="F_XPATH", split_list={"(":0})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h2[@class='detail-bien-ville']/text()", input_type="F_XPATH", split_list={"(":1, ")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//li/span[contains(@class,'surface')]/following-sibling::text()", input_type="F_XPATH", get_num=True, split_list={"m":0})
        
        if response.xpath("//li/span[contains(@class,'chambre')]/following-sibling::text()[not(contains(.,'NC'))]"):
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li/span[contains(@class,'chambre')]/following-sibling::text()[not(contains(.,'NC'))]", input_type="F_XPATH", get_num=True, split_list={" ":0})
        elif response.xpath("//li/span[contains(@class,'piece')]/following-sibling::text()[not(contains(.,'NC'))]"):
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li/span[contains(@class,'piece')]/following-sibling::text()[not(contains(.,'NC'))]", input_type="F_XPATH", get_num=True, split_list={" ":0})
            
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[contains(@class,'prix')]/text()", input_type="F_XPATH", get_num=True, split_list={"€":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//li/span[contains(.,'garantie')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//li//span[contains(.,'charges')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//li[@class='gg-map-marker-lat']", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//li[@class='gg-map-marker-lng']", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'diapo')]//@data-src", input_type="M_XPATH")
        
        desc = " ".join(response.xpath("//span[@itemprop='description']//p/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        import dateparser
        if "Disponible le" in desc:
            available_date = desc.split("Disponible le")[1].split(".")[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        external_id = "+".join(response.xpath("//span[@itemprop='productID']//text()").getall())
        if external_id:
            item_loader.add_value("external_id", external_id.split("+")[1].strip())
        
        energy_label = response.xpath("//img/@src[contains(.,'nrj-w-')]").get()
        if energy_label:
            energy_label = energy_label.split("nrj-w-")[1].split("-")[0]
            item_loader.add_value("energy_label", energy_label)
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Axel Immobilier", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="05 56 12 15 15", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="merignac@axel-immobilier.fr", input_type="VALUE")

        yield item_loader.load_item()