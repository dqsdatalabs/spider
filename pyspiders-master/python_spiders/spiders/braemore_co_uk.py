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
    name = 'braemore_co_uk'
    execution_type='testing' 
    country='united_kingdom'
    locale='en'
    external_source="Braemore_PySpider_united_kingdom"
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://braemore.co.uk/edinburgh-estate-agent/properties-in-edinburgh/?filter_search_type=to-let&filter_property_type=flat-apartment",
                ],
                "property_type": "apartment",
                "prop_type": "flat-apartment"
            },
	        {
                "url": [
                    "https://braemore.co.uk/edinburgh-estate-agent/properties-in-edinburgh/?filter_search_type=to-let&filter_property_type=house"
                ],
                "property_type": "house",
                "prop_type": "house"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type'), 'prop_type': url.get('prop_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[@class='w-grid-list']/article"):
            follow_url = response.urljoin(item.xpath(".//@href[not(contains(.,'#'))]").get())
            status = item.xpath(".//a/span/text()").get()
            if "to let" in status.lower():
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = "https://braemore.co.uk/wp-admin/admin-ajax.php"

            payload = f"action=us_ajax_grid&ajax_url=https%3A%2F%2Fbraemore.co.uk%2Fwp-admin%2Fadmin-ajax.php&infinite_scroll=true&max_num_pages=5&pagination=ajax&permalink_url=https%3A%2F%2Fbraemore.co.uk%2Fedinburgh-estate-agent%2Fproperties-in-edinburgh&template_vars=%7B%22columns%22%3A%223%22%2C%22exclude_items%22%3A%22none%22%2C%22img_size%22%3A%22default%22%2C%22ignore_items_size%22%3Afalse%2C%22items_layout%22%3A%222131%22%2C%22items_offset%22%3A%221%22%2C%22load_animation%22%3A%22afl%22%2C%22overriding_link%22%3A%22none%22%2C%22post_id%22%3A2123%2C%22query_args%22%3A%7B%22post_type%22%3A%5B%22properties%22%5D%2C%22post_status%22%3A%5B%22publish%22%2C%22acf-disabled%22%5D%2C%22post__not_in%22%3A%5B2123%5D%2C%22posts_per_page%22%3A%2212%22%2C%22tax_query%22%3A%7B%22relation%22%3A%22AND%22%7D%2C%22meta_query%22%3A%7B%22relation%22%3A%22AND%22%7D%2C%22paged%22%3A{page}%7D%2C%22orderby_query_args%22%3A%7B%22orderby%22%3A%7B%22date%22%3A%22DESC%22%7D%7D%2C%22type%22%3A%22grid%22%2C%22us_grid_ajax_index%22%3A1%2C%22us_grid_filter_params%22%3A%22filter_search_type%3Dto-let%26filter_property_type%3D{response.meta.get('prop_type')}%22%2C%22us_grid_index%22%3A1%2C%22_us_grid_post_type%22%3A%22properties%22%7D"
            headers = {
            'Connection': 'keep-alive',
            'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
            'Accept': '*/*',
            'X-Requested-With': 'XMLHttpRequest',
            'sec-ch-ua-mobile': '?0',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Accept-Language': 'tr,en;q=0.9,tr-TR;q=0.8,en-US;q=0.7,es;q=0.6,fr;q=0.5,nl;q=0.4',
            'Cookie': '_gcl_au=1.1.66659970.1628687099; _ga=GA1.3.33428518.1628687099; _gid=GA1.3.505602659.1628687099; us_cookie_notice_accepted=true; _gat_gtag_UA_87669493_26=1'
            }
            yield Request(
                url,
                dont_filter=True,
                body=payload,
                method="POST",
                headers=headers, 
                callback=self.parse, 
                meta={
                    "page": page+1, 
                    "property_type": response.meta.get('property_type'),
                    "prop_type": response.meta.get('prop_type')
                }
            ) 

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        external_id = response.xpath("substring-after(//link[@rel='shortlink']/@href,'=')").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        rent = response.xpath("//h2/text()").get()
        if rent:
            rent = rent.strip().split(" ")[0].replace("£","").replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        
        address = " ".join(response.xpath("//div[contains(@id,'address')]/text()").getall())
        if address:
            item_loader.add_value("title", address)
            item_loader.add_value("address", address)
        
        city = response.xpath("//div[contains(@id,'address')]/text()").getall()[-1]
        item_loader.add_value("city", city)
        zipcode=response.xpath("//div[@id='postcode']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode)
        
        room_count = response.xpath("//i[contains(@class,'bed')]/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//i[contains(@class,'bath')]/following-sibling::text()").get()
        if bathroom_count and bathroom_count.strip() !='0':
            item_loader.add_value("bathroom_count", bathroom_count)
        
        parking = response.xpath("//i[contains(@class,'car')]/following-sibling::text() | //li[contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        desc = " ".join(response.xpath("//div[contains(@class,'post_content')]/p//text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
        
        energy_label = response.xpath("//div[contains(@class,'post_content')]/p//text()[contains(.,'EPC')]").get()
        if energy_label:
            energy_label = energy_label.strip().split(" ")[-1]
            item_loader.add_value("energy_label", energy_label)
        
        utilities = response.xpath("//div[contains(@class,'post_content')]/p//text()[contains(.,'Utilities')]").get()
        if utilities:
            utilities = utilities.split(":")[1].strip().split(" ")[0].replace("£","")
            item_loader.add_value("utilities", utilities)
        
        latitude = response.xpath("//span[@id='latitude']/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.strip())
        
        longitude = response.xpath("//span[@id='longitude']/text()").get()
        if longitude:
            item_loader.add_value("longitude", longitude.strip())
        
        furnished = response.xpath("//h3[contains(.,'Furnished')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        washing_machine = response.xpath("//li[contains(.,'Washer') or contains(.,'Washing')]/text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        dishwasher = response.xpath("//li[contains(.,'Dish washer') or contains(.,'Dishwasher')]/text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        images = [x for x in response.xpath("//article[contains(@class,'w-grid-item')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "Braemore Edinburgh")
        item_loader.add_value("landlord_phone", "0131 624 6666")
        item_loader.add_value("landlord_email", "info@braemore.co.uk")
        
        yield item_loader.load_item()