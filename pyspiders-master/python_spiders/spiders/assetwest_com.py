import scrapy
from scrapy.http.request import Request
from ..loaders import ListingLoader
import math
import requests


class AssetwestComSpider(scrapy.Spider):
    name = 'assetwest_com'
    allowed_domains = ['assetwest.com']
    start_urls = ['https://www.assetwest.com/advanced-search/?filter_search_action%5B%5D=&filter_search_type%5B%5D=&advanced_city=&asset-west-id=&min-baths=&min-bedrooms=&min-price-in=&max-price-in=&submit=SEARCH+PROPERTIES&wpestate_regular_search_nonce=af4b93cdbf&_wp_http_referer=%2F']
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    pos = 1

    def parse(self, response):
        for appartment in response.css("#listing_ajax_container > div"):
            url = appartment.css(
                "div.property_listing.property_card_default::attr(data-link)").get()
            yield Request(url,
                          callback=self.populate_item,
                          )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        id = response.xpath("//link[@rel='shortlink']/@href").get()

        title = response.css(
            '#all_wrapper > div > div.container.content_wrapper > div > div.notice_area.col-md-12 > h1::text').get()

        feats = response.css('#collapseOne > div > div')

        bedrooms = None
        bathrooms = None
        available_from = None
        pets = None
        for item in feats:
            if "Bedrooms" in item.css("strong::text").get():
                bedrooms = item.css("div::text").get()
            elif "Bathrooms" in item.css("strong::text").get():
                bathrooms = item.css("div::text").get()
            elif "Available from" in item.css("strong::text").get():
                available_from = item.css("div::text").get()
            elif "Pets Permitted" in item.css("strong::text").get():
                pets = item.css("div::text").get()
                if "No" in pets:
                    pets = False
                else:
                    pets = True

        rent = response.css(
            '#all_wrapper > div > div.container.content_wrapper > div > div.notice_area.col-md-12 > div.price_area::text').get().split(" ")[1].strip()
        try:
            if "," in rent:
                rent = rent.split(",")
                rent = rent[0]+rent[1]
        except:
            rent = rent

        description = ''
        description_array = response.css(
            "#wpestate_property_description_section > p::text").extract()

        if description_array == []:
            description_array = response.css(
                '#listingview_full_desc > p::text').extract()

        for item in description_array:
            description += item

        images = response.css(
            'a.prettygalery::attr(href)').extract()


        furnished = response.css("div.property_title_label > a::text").get()
        if "Unfurnished" in furnished:
            furnished = False
        elif "furnished" in furnished.lower():
            furnished = True
        else:
            furnished = None

        washing_machine = None
        if 'laundry' in description.lower():
            washing_machine = True

        lat = response.css("#gmap_wrapper::attr(data-cur_lat)").get()
        lng = response.css("#gmap_wrapper::attr(data-cur_long)").get()

        responseGeocode = requests.get(
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={lng},{lat}&f=pjson&distance=50000&outSR=")
        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        city = responseGeocodeData['address']['City']
        address = responseGeocodeData['address']['Match_addr']

        balcony = None
        if "balcony" in description:
            balcony = True

        parking = None
        if "parking" in description:
            parking = True

        if 'yorkville' in title.lower():
            city = 'Yorkville'

        # MetaData
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value(
            "external_id", "{}".format(id.split("=")[-1].strip()))
        item_loader.add_value("title", title)
        item_loader.add_value("description", description)

        # Property Details
        item_loader.add_value("property_type", "apartment")
        # item_loader.add_value("square_meters", int(int(int(space))*10.764))
        item_loader.add_value("room_count", bedrooms)
        item_loader.add_value("bathroom_count", math.ceil(float(bathrooms)))
        item_loader.add_value("address", address)
        item_loader.add_value("city", city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("available_date", available_from)
        item_loader.add_value("furnished", furnished)
        item_loader.add_value("parking", parking)
        item_loader.add_value("pets_allowed", pets)
        item_loader.add_value("balcony", balcony)
        item_loader.add_value("washing_machine", washing_machine)

        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", lng)

        # Images
        item_loader.add_value("images", images)
        item_loader.add_value("external_images_count", len(images))

        # Monetary Status
        item_loader.add_value("rent", int(rent))
        item_loader.add_value("currency", "CAD")

        # # LandLord Details
        item_loader.add_value(
            "landlord_name", 'Asset West Property Management')
        item_loader.add_value("landlord_phone", '403-290-1912')
        # item_loader.add_value("landlord_email", landlord_email)

        item_loader.add_value("position", self.pos)

        self.pos += 1

        yield item_loader.load_item()
