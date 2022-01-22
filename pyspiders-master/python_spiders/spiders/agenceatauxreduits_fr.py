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
import dateparser
from python_spiders.helper import remove_white_spaces,extract_number_only
class MySpider(Spider):
    name = 'agenceatauxreduits_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Agenceatauxreduits_PySpider_france"
    # start_urls = ['https://www.agenceatauxreduits.fr/results']  # LEVEL 1
    # post_url = "https://www.agenceatauxreduits.fr/results"
    current_index = 0
    # other_prop = ["1,maison"]
    # other_prop_type = ["house"]
    position = 0
    custom_settings = {              
    "PROXY_FR_ON" : True,
    "CONCURRENT_REQUESTS": 3,        
    "COOKIES_ENABLED": False,        
    "RETRY_TIMES": 3,        
    }
    download_timeout = 120
    headers ={
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36',
    }
    def start_requests(self):
        formdata = {
            "choix_frm_select_type": "2",
            "site_frm_select_type": "",
            "dpt_name": "",
            "dpt_num": "",
            "site_frm_select_commune": "",
            "site_frm_budget_vente": "",
            "site_frm_budget_loc": "",
            "site_frm_ref": "",
        }
        yield FormRequest(
            url="https://www.agenceatauxreduits.fr/results",
            callback=self.parse,
            headers=self.headers,
            method="POST",
            dont_filter=True,
            formdata=formdata,
        )


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='listItem']"):
            follow_url = response.urljoin(item.xpath(".//a/@href").get())
            # address = item.xpath(".//h3/text()").get()
            yield Request(follow_url, callback=self.populate_item,headers=self.headers)
            seen = True
        
        # if self.current_index < len(self.other_prop):
        #     formdata = {
        #     "choix_frm_select_type": "2",
        #     "site_frm_select_type": self.other_prop[self.current_index],
        #     "dpt_name": "",
        #     "dpt_num": "",
        #     "site_frm_select_commune": "",
        #     "site_frm_budget_vente": "",
        #     "site_frm_budget_loc": "",
        #     "site_frm_ref": "",
        # }
        
        #     yield FormRequest(
        #         url=self.post_url,
        #         callback=self.parse,
        #         dont_filter=True,
        #         formdata=formdata,
        #     )
        #     self.current_index += 1

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        print("-----------")
        # item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//h1/text()[normalize-space()]").extract_first()
        if title:
            item_loader.add_value("title", remove_white_spaces(title.title()))
        room_count = response.xpath("//div[@class='itemContainer chambre']/span/text()").extract_first()
        if room_count:
            item_loader.add_value("room_count", extract_number_only(room_count))
        bathroom_count = response.xpath("//div[@class='itemContainer douche']/span/text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count", extract_number_only(bathroom_count))
        parking = response.xpath("//div[@class='itemContainer garage']/span/text()").extract_first()
        if parking:
            item_loader.add_value("parking", True)

        square_meters = response.xpath("//div[@class='itemContainer surface']/span/text()").extract_first()
        if square_meters:
            item_loader.add_value("square_meters", extract_number_only(square_meters))
       
        item_loader.add_xpath("external_id", "//span[strong[.='Référence']]/text()")

        rent = "".join(response.xpath("//span[@class='prix']/strong/text()").extract())
        if rent:
            item_loader.add_value("rent_string", rent)

        desc = " ".join(response.xpath("//p[@class='descTxt']/text()").extract())
        if desc:
            item_loader.add_value("description", remove_white_spaces(desc))

        address = response.meta.get("address")
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", address.split("(")[-1].split(")")[0].strip())
            item_loader.add_value("city", address.split("(")[0].strip())

        images = [x for x in response.xpath("//div[@id='container-visuels']//a/@href").extract()]
        if images:
            item_loader.add_value("images", images) 

        self.position += 1
        item_loader.add_value('position', self.position)
        landlord_name = response.xpath("//div[@class='nego']/span/text()").extract_first()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.strip())
        landlord_phone = response.xpath("//div[@class='nego']//a[@class='phone']/text()").extract_first()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())
        item_loader.add_value("landlord_email", "contact@agenceatauxreduits.fr")
        
        yield item_loader.load_item()