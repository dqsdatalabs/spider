# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'thehaguerealestate_com'
    start_urls = ['https://www.thehaguerealestate.nl/wp-admin/admin-ajax.php?action=doSearch&job=exclude_posts%3D%26tiara_object_ownership_type%3D2%26lat_lng%3D%26tiara_object_city%3D%26tiara_object_address_street%3D%26tiara_object_price_min%3D1%26tiara_object_price_max%3D10000000%26tiara_object_building_classname%3D%26tiara_object_facilities%3D%26ls%3D%26relation%3D%26relation_type%3D%26tiara_object_building_group%3D%26tiara_object_building_type%3D%26tiara_object_building_construction_period%3D%26tiara_object_building_floor_usage%3D%26tiara_object_building_plot_size%3D%26tiara_object_building_bedrooms%3D%26tiara_object_building_situations%3D%26tiara_object_main_garden_situation%3D%26tiara_object_acceptance%3D%26tiara_object_building_condition%3D%26tiara_object_anomalities%3D%26tiara_fieldset_type%3D%26tiara_object_garages%3D%26tiara_object_insulation%3D%26open_huis%3D%26tiara_object_status_code%3D0%26omode%3Dparse%26orderby%3DDATE&amount=150&skip=false&pagination=false&simpleresults=true']  # LEVEL 1
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source = "Thehaguerealestate_PySpider_netherlands_nl"

    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'grid-column col-4 feature bsc')]"):
            follow_url = response.urljoin(item.xpath("@data-link").extract_first())
            if "woonhuis" in follow_url:
                property_type = "house"
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": property_type})
            elif "appartement" in follow_url:
                property_type = "apartment"
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": property_type})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Thehaguerealestate_PySpider_" + self.country + "_" + self.locale)

        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        item_loader.add_value("external_link", response.url)
        
        desc = "".join(response.xpath("//div[@id='property-description']//div[contains(@class,'grid-column col-8 property-section-content-inner contentarea pr30')]//text()").extract())
        desc = re.sub('\s{2,}', ' ', desc)
        item_loader.add_value("description", desc.strip())
        
        latitude = response.xpath("//div[@class='grid-wrapper property-map']/@data-lat").get()
        longitude = response.xpath("//div[@class='grid-wrapper property-map']/@data-lng").get()

        if latitude and longitude:
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        address = " ".join(response.xpath("//div[contains(@class,'property-alldetails-title')]/span/span//text()").extract())
        item_loader.add_value("address", address)
        city = " ".join(response.xpath("//div[contains(@class,'property-alldetails-title')]/span/span[last()]//text()").extract())
        if city:    
            item_loader.add_value("city", city)

        item_loader.add_value("property_type", response.meta.get("property_type"))
        
        square_meters = response.xpath("normalize-space(//div[./span[contains(.,'Woonoppervlakte')]]/span[2]/text())").get()
        if square_meters:
            square_meters = square_meters.split(" ")[0]
        item_loader.add_value("square_meters", square_meters)
        

        room_count = response.xpath("normalize-space(//div[./span[contains(.,'Aantal kamers')]]/span[2]/text())").get()
        if room_count:
            room_count = room_count.split(" ")[0].strip()
            # if "studio" in item_loader.get_collected_values("property_type") and "0" in room_count:
            #     item_loader.add_value("room_count", "1")
            # else:
            if "0" not in room_count:
                item_loader.add_value("room_count", room_count)
        bathroom_count = response.xpath("normalize-space(//div[./span[contains(.,'Badkamers')]]/span[2]/text())").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split(" ")[0].strip()
            if "0" not in bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count)
        elif not bathroom_count:
            try:
                if desc:
                    if " BADKAMERS" in desc:
                        bath = desc.split(" BADKAMERS")[0].strip().split(" ")[-1].strip()
                        if bath.isdigit():
                            item_loader.add_value("bathroom_count", bath)
            except:
                pass
        images = [x for x in response.xpath("//div[@class='grid-column col-8 property-pictures-inner masonry-grid-inner type-7']/div/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        else:
            images = [x for x in response.xpath("//div[@class='image-box']//@data-lazy-src").getall()]
            item_loader.add_value("images", images)
        
        
        available_date = response.xpath("normalize-space(//div[./span[contains(.,'Oplevering')]]/span[2]/text())").get()
        if available_date and available_date.replace(" ","").isalpha() != True:
            try:
                available_date = available_date.split(",")[1].strip()
            except:
                pass
            date_parsed = dateparser.parse(available_date, date_formats=["%d-%m-%Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)

        price = response.xpath("//div[@class='woning-price extra-bold fs20 csc text-shadow margin0 pt10']/span[contains(.,'€')]/text()").get()
        if price:
            item_loader.add_value("rent_string", price)
        
        deposit = response.xpath("//div[./span[contains(.,'Waarborgsom')]]/span[2]/span/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.strip("€").strip())
        utilities = response.xpath("//div[./span[contains(.,'Servicekosten')]]/span[2]/span/text()").get()
        if utilities:
            utilities = int(float(utilities.strip("€").strip()))
            item_loader.add_value("utilities", utilities)
        
        furnished = " ".join(response.xpath("//div[./span[contains(.,'Bijzonderheden')]]/span[2]/span/text()").getall())
        if furnished:
            if "Gestoffeerd" in furnished or "Gemeubileerd" in furnished:
                item_loader.add_value("furnished", True)
            

        parking = response.xpath("//div[./span[contains(.,'Garage')]]/span[2]/span/text()").get()
        if parking:
            if "Geen" in parking:            
                item_loader.add_value("parking", False)
            else:           
                item_loader.add_value("parking", True)
        balcony = response.xpath("//div[./span[contains(.,'balkon')]]/span[2]/span/text()").get()
        if balcony:          
            item_loader.add_value("balcony", True)
        elevator = response.xpath("//div[./span[contains(.,'Voorzieningen')]]/span//text()[contains(.,'Lift')]").getall()
        if elevator:
            item_loader.add_value("elevator", True)
                
        energy_label = response.xpath("normalize-space(//div[./span[contains(.,'Energielabel')]]/span[2]/text())").get()
        if energy_label and energy_label != "Niet beschikbaar":
            item_loader.add_value("energy_label", energy_label)

        item_loader.add_value("landlord_phone", "+31 70 21 41 077")
        item_loader.add_value("landlord_email", "info@thres.nl")
        item_loader.add_value("landlord_name", "The Hague Real Estate Services")
        
        status = response.xpath("//div[span[contains(.,'Status')]]/span[2]/text()").get()
        if status and ("Verkocht" in status or "Verhuurd" in status):
            return
        yield item_loader.load_item()