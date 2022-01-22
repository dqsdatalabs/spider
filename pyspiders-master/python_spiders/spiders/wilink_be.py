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
class MySpider(Spider):
    name = 'wilink_be'
    execution_type='testing'
    country='belgium'
    locale='nl'
    external_source='Wilink_PySpider_belgium'
    start_urls = ["https://www.wilinkrealestate.be/huren"]
    custom_settings = {
        # "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,
        "HTTPERROR_ALLOWED_CODES": [301,302,400,401,406,403,503]
    }
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for follow_url in response.xpath("//div[@class='col-sm-4 mb-4 px-2']/a/@href").getall():
            yield Request(response.urljoin(follow_url), callback=self.populate_item)
        if page == 2 or seen:
            headers={
                ":authority": "www.wilinkrealestate.be",
                ":method": "POST",
                ":path": "/huren?ajax_form=1&_wrapper_format=drupal_ajax",
                ":scheme": "https",
                "accept": "application/json, text/javascript, */*; q=0.01",
                "accept-language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
                "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
                "cookie": "cookie-agreed-version=1.0.0; _fbp=fb.1.1641792073128.973905895; cookie-agreed=2; _gcl_au=1.1.1185465652.1641792099; _ga=GA1.1.339874846.1641792099; _ga_4E6BF5PB70=GS1.1.1641791796.2.1.1641792112.0",
                "origin": "https://www.wilinkrealestate.be",
                "referer": "https://www.wilinkrealestate.be/huren",
                "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Mobile Safari/537.36",
                "x-requested-with": "XMLHttpRequest",
            }
            formdata={
                "form_build_id": "form-jfIQpMVrxbnU_nl4kil4odJ6i-wxAxWN9uN_6XKEZT0",
                "form_id": "huren_filter_form",
                "access_kye": "",
                "PurposeStatusIds": "2",
                "City": "",
                "price_min": "",
                "price_max": "",
                "Rooms": "",
                "sort": "false",
                "_triggering_element_name": "previous",
                "_triggering_element_value": "Vorige",
                "_drupal_ajax": "1",
                "ajax_page_state[theme]": "wilink",
                "ajax_page_state[theme_token]": "",
                "ajax_page_state[libraries]": "bootstrap4/bootstrap4-js-latest,bootstrap4/global-styling,classy/base,classy/messages,core/normalize,eu_cookie_compliance/eu_cookie_compliance_default,fontawesome/fontawesome.webfonts.shim,properties/properties,system/base,wilink/global-styling"
            }
            follow_url = "https://www.wilinkrealestate.be/huren?ajax_form=1&_wrapper_format=drupal_ajax&_wrapper_format=drupal_ajax"
            yield FormRequest(follow_url, formdata=formdata,headers=headers,callback=self.parse, meta={"page": page + 1})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//h1[@class='font-weight-bold']/text()").get()
        if title:
            item_loader.add_value('title',title)
        rent=response.xpath("//div[@class='price pb-2']/p/text()").get()
        if rent and not "PRIJS"==rent:
            item_loader.add_value("rent",rent.split("€")[-1].split("/ma")[0].replace(" ","").replace(" ","").replace(".","").replace(",",""))
        item_loader.add_value("currency","EUR")
        adres=response.xpath("//div[@class='address py-2']/p/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        square_meters=response.xpath("//p[@class='text-sell']/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0])
        description="".join(response.xpath("//div[@class='description pb-4 pt-5']/text()").getall())
        if description:
            item_loader.add_value("description",description)
        elevator=response.xpath("//td[.='lift']/following-sibling::td/text()").get()
        if elevator and elevator=="Ja":
            item_loader.add_value("elevator",True)
        terrace=response.xpath("//td[.='terras 1']/text()").get()
        if terrace:
            item_loader.add_value("terrace",True)
        bathroom_count=response.xpath("//p[contains(.,'badkamer')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip().split(" ")[0])
        room_count=response.xpath("//p[contains(.,'slaapkamers')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split(" ")[0])
        images=[x for x in response.xpath("//a[@class='item']/@href").getall()]
        if images:
            item_loader.add_value("images",images)
        parking=response.xpath("//td[.='parking binnen']/following-sibling::td/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        energy_label=response.xpath("//td[.='EPC-klasse']/following-sibling::td/text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label)
        name=response.xpath("//p[@class='font-weight-bold title']/text()").get()
        if name:
            item_loader.add_value("landlord_name",name)
        phone=response.xpath("//a[contains(@href,'tel')]/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone)

        yield item_loader.load_item() 