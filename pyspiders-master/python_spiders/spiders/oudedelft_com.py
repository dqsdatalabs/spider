# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math
import dateparser

class MySpider(Spider):
    name = 'oudedelft_com'
    execution_type = 'testing' 
    country = 'netherlands'
    locale = 'nl'
    external_source='Oudedelft_PySpider_netherlands_nl'
    def start_requests(self):
        for i in range(1, 7):
            headers = {
                "content-type": "application/json;charset=UTF-8",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
                "origin": "https://oudedelft.com"
            }
           
            payload = {"post_type":"post","limit":"200","page":1,"q":[{"ID":"category","filter_as":"checkbox_post_terms","group_type":"taxonomies","type":"checkbox_post_terms","value":["26"],"variation_id":"null"},{"ID":"post_folder","filter_as":"checkbox_post_terms","group_type":"taxonomies","type":"checkbox_post_terms","value":["53"],"variation_id":"null"}],"is_wpml":0,"lang":"","filter_id":"snoafjglwucljfl_4"}
            payload["page"] = i
            
            url = "https://oudedelft.com/wp-json/lscf_rest/filter_posts"
            yield Request(url, self.parse, method="POST", headers=headers, body=json.dumps(payload))
    
    # 1. FOLLOWING
    def parse(self, response):
        
        data = json.loads(response.body) 
        
        for item in data["posts"]:
            follow_url = item["permalink"]

            if item.get("customFields", None):
                if "soort_woning__pxid_qcfgkfuorglluis_1" in item["customFields"].keys():
                    if "value" in item["customFields"]["soort_woning__pxid_qcfgkfuorglluis_1"].keys():
                        prop = item["customFields"]["soort_woning__pxid_qcfgkfuorglluis_1"]["value"]
                        if "Appartement" in prop:
                            property_type = "apartment"
                        elif "Woonhuis" in prop:
                            property_type = "house"
                        yield Request(follow_url, callback=self.populate_item, meta={"property_type": property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Oudedelft_PySpider_" + self.country + "_" + self.locale)
        
        title = " ".join(response.xpath("//div[contains(@class,'et_pb_text_0_tb_body')]/div[@class='et_pb_text_inner']/text()").extract())
        if title:
            item_loader.add_value("title", title) 
        else:
            item_loader.add_xpath("title", "//title/text()")
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("external_id", "substring-after(//link[@rel='shortlink']/@href,'?p=')")

        item_loader.add_value("property_type", response.meta.get("property_type"))

        desc = " ".join(response.xpath("//div[@class='et_pb_row_inner et_pb_row_inner_7_tb_body']//div[@class='et_pb_text_inner']//text()").extract())
        item_loader.add_value("description", desc.strip())

        price = response.xpath("(//div[@class='et_pb_text_inner']//text()[contains(.,'€')])[1]").get()
        if price: 
            if "€€" in price:
                item_loader.add_value("rent", price.split(',')[0].split('€€')[1].strip().replace(".",""))
            elif "," in price:
                item_loader.add_value("rent", price.split("€")[1].split(",")[0].replace(".","").split(" ")[0])
            elif "-" in price:
                item_loader.add_value("rent", price.split("€")[1].replace(".","").split(" ")[0].replace("-",""))
            else:
                item_loader.add_value("rent", price.split("€")[1].split(" ")[0].replace(".","").split(" ")[0])
        item_loader.add_value("currency", "EUR")

        square = response.xpath(
            "//div[contains(@class,'et_pb_with_border ')][*//p[contains(.,'Oppervlakte')]]/div[2]//text()[normalize-space()]"
        ).get()
        if square:
            if square != "m²":
                if "m2m²" in square:
                    square = square.split("m2m²")[0].strip()
                else:
                    square = square.split("m²")[0].strip()
                if "," in square:
                    square = square.replace(",",".")
                    square = math.ceil(float(square))
                item_loader.add_value("square_meters", str(square))
        room_count = response.xpath(
            "//div[contains(@class,'et_pb_with_border ')][*//p[contains(.,' kamer')]]/div[2]//text()[normalize-space()]"
        ).get()
        if room_count:
            if room_count.strip() != "kamer (s)":
                item_loader.add_value("room_count", room_count.split(" ")[0])

        street = response.xpath("//div[contains(@class,'et_pb_text_1_tb_body')]/div/text()").get()
        city = response.xpath("//div[contains(@class,'et_pb_text_2_tb_body')]/div/text()").get()
        
        item_loader.add_value("address", f"{street} {city}".strip())
        item_loader.add_value("zipcode", street.split(" ")[0])
        item_loader.add_value("city", city)
            
        available_date = response.xpath(
            "//div[contains(@class,'et_pb_with_border ')][*//p[contains(.,'Beschikbaar per')]]/div[2]//text()[normalize-space()]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        images = [response.urljoin(x) for x in response.xpath('//a[@class="dsm_image_carousel_lightbox"]/@href').getall()]
        if images:
            item_loader.add_value("images", images)
        else: 
            images = [response.urljoin(x) for x in response.xpath("//div[@class='fagsfacf-slider']//img/@data-src").extract()]
            if images:
                item_loader.add_value("images", images)
            else:
                images = [response.urljoin(x) for x in response.xpath("//meta[@property='og:image']//@content").getall()]
                item_loader.add_value("images", images)
        
        # latlng = response.xpath("//h3/a[contains(@href,'/maps/') and contains(@href,'/@')]/@href").get()
        # if latlng:
        #     item_loader.add_value("latitude", latlng.split("/@")[1].split(",")[0].strip())
        #     item_loader.add_value("longitude", latlng.split("/@")[1].split(",")[1].strip())

        furnished  = "".join(response.xpath("//div[@class='et_pb_row_inner et_pb_row_inner_7_tb_body']//div[@class='et_pb_text_inner']//text()").extract())
        furnished = furnished.replace("\n","")
        if furnished:
            if "gemeubileerd" in furnished:
                item_loader.add_value("furnished", True)
            elif "Furnished" in furnished:
                item_loader.add_value("furnished", True)
        landlord_name  = response.xpath("//div[@id='sticky']/div/div[p/a]/h3/text()").get()
        if landlord_name:
            item_loader.add_xpath("landlord_phone", "//div[@id='sticky']//a[contains(@href,'tel')]/text()")
            item_loader.add_xpath("landlord_name", "//div[@id='sticky']/div/div[p/a]/h3/text()")
            item_loader.add_xpath("landlord_email", "//div[@id='sticky']//a/text()[contains(.,'@')]")
        else:
            item_loader.add_value("landlord_phone", "+31 (0)15 213 1110")
            item_loader.add_value("landlord_name", "Oude Delft")
            item_loader.add_value("landlord_email", "info@oudedelft.com")
        yield item_loader.load_item() 