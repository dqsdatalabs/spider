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
    name = 'daviesdavies_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.daviesdavies.co.uk/property/?wppf_search=to-rent&wppf_property_type=flat&wppf_orderby=latest&wppf_view=list&wppf_lat=0&wppf_lng=0&wppf_radius=10&wppf_records=24",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.daviesdavies.co.uk/property/?wppf_search=to-rent&wppf_property_type=house-terraced&wppf_orderby=latest&wppf_view=list&wppf_lat=0&wppf_lng=0&wppf_radius=10&wppf_records=24",
                    "https://www.daviesdavies.co.uk/property/?wppf_search=to-rent&wppf_property_type=maisonette&wppf_orderby=latest&wppf_view=list&wppf_lat=0&wppf_lng=0&wppf_radius=10&wppf_records=24",
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
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//figure"):
            status = item.xpath("./figcaption/h5/text()").get()
            room_count = item.xpath("./../..//label[contains(.,'Bedroom')]/../text()").get()
            bathroom_count = item.xpath("./../..//label[contains(.,'Bathroom')]/../text()").get()
            if status and "agreed" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"], "room_count": room_count, "bathroom_count": bathroom_count})
            seen = True

        if page == 2 or seen:
            p_url = f"https://www.daviesdavies.co.uk/property/page/{page}/?" + response.url.split("?")[1]
            yield Request(
                p_url,
                callback=self.parse,
                meta={"property_type":response.meta["property_type"], "page":page+1}
            )    
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        room_count = response.meta.get("room_count")
        bathroom_count = response.meta.get("bathroom_count")
    
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Daviesdavies_Co_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[contains(@class,'description')]//h3[contains(.,'About the Property')]/following-sibling::*//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value=room_count, input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value=bathroom_count, input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//h1/br/following-sibling::text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='wppf_slideshow']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//h3[contains(.,'Floorplan')]/following-sibling::a/@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'LatLng')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'LatLng')]/text()", input_type="F_XPATH", split_list={"LatLng(":1, ",":1, ")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//li[contains(.,'EPC rating')]/text()", input_type="F_XPATH", split_list={"rating":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(.,'Balcony') or contains(.,'balcony')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[contains(.,'Terrace') or contains(.,'terrace')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Davies & Davies Estate Agents", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="020 7272 0986", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@daviesdavies.co.uk", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Unfurnished') or contains(.,'unfurnished')]", input_type="F_XPATH", tf_item=True, tf_value=False)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished') or contains(.,'furnished')]", input_type="F_XPATH", tf_item=True, tf_value=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'parking')]/text()", input_type="F_XPATH", tf_item=True)

        from datetime import datetime
        import dateparser
        available_date = response.xpath("//li[contains(.,'Available')]/text()").get()
        if available_date:
            available_date = available_date.split("Available")[-1].replace("from","").strip()
            if "now" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)


        yield item_loader.load_item()