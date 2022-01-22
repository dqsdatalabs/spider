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
    name = 'bernardsestates_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source="Bernardsestates_PySpider_united_kingdom_en"

    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36",
        }

    def start_requests(self):
        

        start_urls = [
            {
                "url" : "https://bernardsestates.co.uk/property-search/?location-search=&be-tab-selector=to-let&wpv_view_count=1205-CATTRb023de73e0b9f7c57ca676823425ff58&toolset_maps_distance_center=&toolset_maps_distance_radius=1&toolset_maps_distance_unit=mi&wpv-wpcf-property-type=Houses&wpv-wpcf-rental-price_min=0&wpv-wpcf-rental-price_max=99999999&wpv-wpcf-bedrooms-min=&wpv-wpcf-bedrooms-max=&wpv_filter_submit=Search&wpv-wpcf-property-available=available&wpv_paged=1",
                "property_type" : "house"
            },
            {
                "url" : "https://bernardsestates.co.uk/property-search/?location-search=&be-tab-selector=to-let&wpv_view_count=1205-CATTRb023de73e0b9f7c57ca676823425ff58&toolset_maps_distance_center=&toolset_maps_distance_radius=1&toolset_maps_distance_unit=mi&wpv-wpcf-property-type=Flats+%2F+Apartments&wpv-wpcf-rental-price_min=0&wpv-wpcf-rental-price_max=99999999&wpv-wpcf-bedrooms-min=&wpv-wpcf-bedrooms-max=&wpv_filter_submit=Search&wpv-wpcf-property-available=available&wpv_paged=1",
                "property_type" : "apartment"
            },
        ]
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            headers=self.headers,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("(//div[@class='be-properties-inner'])[1]//div[@class='be-visual-menu-item-inner']//following-sibling::a//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True

        if page == 2 or seen:
            f_url = ''
            if 'Houses' in response.url:
                f_url = f'https://bernardsestates.co.uk/property-search/?location-search=&be-tab-selector=to-let&wpv_view_count=1205-CATTRb023de73e0b9f7c57ca676823425ff58&toolset_maps_distance_center=&toolset_maps_distance_radius=1&toolset_maps_distance_unit=mi&wpv-wpcf-property-type=Houses&wpv-wpcf-rental-price_min=0&wpv-wpcf-rental-price_max=99999999&wpv-wpcf-bedrooms-min=&wpv-wpcf-bedrooms-max=&wpv_filter_submit=Search&wpv-wpcf-property-available=available&wpv_paged={page}'
            else:
                f_url = f'https://bernardsestates.co.uk/property-search/?location-search=&be-tab-selector=to-let&wpv_view_count=1205-CATTRb023de73e0b9f7c57ca676823425ff58&toolset_maps_distance_center=&toolset_maps_distance_radius=1&toolset_maps_distance_unit=mi&wpv-wpcf-property-type=Flats+%2F+Apartments&wpv-wpcf-rental-price_min=0&wpv-wpcf-rental-price_max=99999999&wpv-wpcf-bedrooms-min=&wpv-wpcf-bedrooms-max=&wpv_filter_submit=Search&wpv-wpcf-property-available=available&wpv_paged={page}'
            yield Request(
                url=f_url,
                callback=self.parse,
                headers=self.headers,
                meta={
                    "property_type" : response.meta.get("property_type"),
                    "page" : page+1,
                }
            )
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        address = response.xpath("//div[@class='be-prop-title']/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
        
        if "," in address:
            item_loader.add_value("city", address.split(",")[-1].strip())
        else:
            item_loader.add_value("city", address.replace("\n","").replace("\t",""))
        
        latitude = response.xpath("//div[@id='street-view']/div[2]/@data-markerlat").get()
        longitude = response.xpath("//div[@id='street-view']/div[2]/@data-markerlon").get()
        if latitude and longitude:
            latitude = latitude.strip()
            longitude = longitude.strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        description = response.xpath("//div[@id='property-detail']/p/text()").getall()
        if description:
            desc = " ".join(description)
            item_loader.add_value("description", desc)

        if 'sq ft' in desc.lower() or 'sq. ft.' in desc.lower() or 'sqft' in desc.lower():
            square_meters = desc.lower().split('sq ft')[0].split('sq. ft.')[0].split('sqft')[0].strip().replace('\xa0', '').split(' ')[-1]
            square_meters = str(int(float(square_meters.replace(',', '.').strip('+')) * 0.09290304))
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//div[contains(text(),'Bedroom')]/text()").get()
        if room_count:
            room_count = room_count.strip().replace('\xa0', '').split(' ')[0].strip()
            room_count = str(int(float(room_count)))
            item_loader.add_value("room_count", room_count)
            
        bathroom_count = response.xpath("//div[contains(text(),'Bathroom')]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().replace('\xa0', '').split(' ')[0].strip()
            bathroom_count = str(int(float(bathroom_count)))
            item_loader.add_value("bathroom_count", bathroom_count)
            
        rent = response.xpath("//div[@class='be-prop-price']/text()").get()
        if rent:
            if rent and "," in rent:
                rent = rent.split(",")[0]
            rent = rent.split('£')[1].split('p')[0].strip().replace('\xa0', '').replace(',', '')

            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", 'GBP')

        images = [x.split('url(')[-1] for x in response.xpath("//div[@id='be-single-property-gallery-slider']/div/@style").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        floor_plan_images = [x for x in response.xpath("//img[contains(@data-src,'loorplan')]/@data-src").getall()]
        if floor_plan_images: 
            item_loader.add_value("floor_plan_images", floor_plan_images)

        energy_label = response.xpath("//img[contains(@src,'hip/EE')]/@src").get()
        if energy_label:
            energy_label = int(energy_label.split('/')[-1].split('_')[2].strip())
            if energy_label >= 92:
                energy_label = 'A'
            elif energy_label >= 81 and energy_label <= 91:
                energy_label = 'B'
            elif energy_label >= 69 and energy_label <= 80:
                energy_label = 'C'
            elif energy_label >= 55 and energy_label <= 68:
                energy_label = 'D'
            elif energy_label >= 39 and energy_label <= 54:
                energy_label = 'E'
            elif energy_label >= 21 and energy_label <= 38:
                energy_label = 'F'
            elif energy_label >= 1 and energy_label <= 20:
                energy_label = 'G'
            item_loader.add_value("energy_label", energy_label)

        furnished = response.xpath("//div[@id='property-detail']/ul/li[contains(.,'FURNISHED')]/span/text()").get()
        if furnished:
            furnished = True
            item_loader.add_value("furnished", furnished)
        
        item_loader.add_value("landlord_name", "BERNARDS THE ESTATE AGENTS")
        
        phone = response.xpath("//div[contains(@class,'normal')][2]/text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone)
            
        item_loader.add_value("landlord_email", "southsea@bernardsestates.co.uk")
        
        yield item_loader.load_item()


