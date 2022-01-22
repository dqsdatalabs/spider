# -*- coding: utf-8 -*-
# Author: Asmaa Elshahat
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_location_from_address


class KontaktwohnungsbaugenossenschaftPyspiderGermanySpider(scrapy.Spider):
    name = "KontaktWohnungsbauGenossenschaft"
    start_urls = ['https://www.wbg-kontakt.de/vermietung/wohnungssuche?sort=&reverse=&perPage=&wohnflaeche=%2C&zimmer=%2C&kaltmiete=%2C#listcontainer']
    allowed_domains = ["wbg-kontakt.de"]
    country = 'germany' # Fill in the Country's name
    locale = 'de' # Fill in the Country's locale, look up the docs if unsure
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    position = 1

    # 1. SCRAPING level 1
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    # 2. SCRAPING level 2
    def parse(self, response, **kwargs):
        pages_number = response.css('div#immolistPagination div.block-pagination ul li.last a::attr(href)')[0].extract()
        pages_number = pages_number.split("page=")[1]
        pages_number = int(pages_number)
        urls = ['https://www.wbg-kontakt.de/vermietung/wohnungssuche?sort=&reverse=&perPage=&wohnflaeche=%2C&zimmer=%2C&kaltmiete=%2C#listcontainer']
        for i in range(pages_number-1):
            page_url = 'https://www.wbg-kontakt.de/vermietung/wohnungssuche?params%5Bpage%5D=641&params%5Baction%5D=immosearchresultcount&params%5Bmid%5D=346&params%5Brollstuhl%5D=false&params%5Bbarrierearm%5D=false&params%5Baufzug%5D=false&params%5Bbalkon%5D=false&params%5Babsofort%5D=false&params%5Bkueche%5D=false&params%5BpageNumber%5D=1&params%5Bsort%5D=&params%5Breverse%5D=&params%5BperPage%5D=&params%5Bparams%5Baufzug%5D%5D=&params%5Bparams%5Bbalkon%5D%5D=&params%5Bparams%5Bbarrierearm%5D%5D=&params%5Bparams%5Babsofort%5D%5D=&params%5Bparams%5Bkueche%5D%5D=&params%5Bwohnflaeche%5D=%2C&params%5Bzimmer%5D=%2C&params%5Bkaltmiete%5D=%2C&page=' + str(i+2)
            urls.append(page_url)
        for url in urls:
            yield scrapy.Request(url, dont_filter=True, callback=self.parse_pages)

    def parse_pages(self, response):
        apartments_divs = response.css('div.immo_list_item div.row')
        for apartment_div in apartments_divs:
            title = apartment_div.css('div.object_data h2.immo_heading a::text')[0].extract()
            apartment_url = apartment_div.css('div.object_data h2.immo_heading a::attr(href)')[0].extract()
            url = 'https://www.wbg-kontakt.de/' + apartment_url
            district = apartment_div.css('div.object_data div.object_address span.district::text').extract()
            if len(district) >= 1:
                district = district[0]
            else:
                district = None
            street = apartment_div.css('div.object_data div.object_address span.street::text').extract()
            if len(street) >= 1:
                street = street[0]
            else:
                street = None
            house_number = apartment_div.css('div.object_data div.object_address span.house_number::text')[0].extract()
            zipcode = apartment_div.css('div.object_data div.object_address span.postal::text')[0].extract()
            region = apartment_div.css('div.object_data div.object_address span.region::text')[0].extract()
            rent = apartment_div.css('div.object_data div.object_details ul li.detail_lease div.fieldValue::text')[0].extract()
            square_meters = apartment_div.css('div.object_data div.object_details ul li.detail_surface div.fieldValue::text')[0].extract()
            room_count = apartment_div.css('div.object_data div.object_details ul li.detail_rooms div.fieldValue::text')[0].extract()
            floor = apartment_div.css('div.object_data div.object_details ul li.detail_floor div.fieldValue::text')[0].extract()
            amenities = apartment_div.css('div.object_data div.object_details ul li.detail_floor div.fieldLabel::text').extract()
            yield scrapy.Request(url, callback=self.populate_item, meta={
                'title': title,
                'district': district,
                'street': street,
                'house_number': house_number,
                'zipcode': zipcode,
                'region': region,
                'rent': rent,
                'floor': floor,
                'amenities': amenities,
                'square_meters': square_meters,
                'room_count': room_count,
            })

    # 3. SCRAPING level 3
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = response.meta.get('title')
        title = title.strip()

        district = response.meta.get('district')
        street = response.meta.get('street')
        house_number = response.meta.get('house_number')
        zipcode = response.meta.get('zipcode')
        region = response.meta.get('region')
        city = region
        if district:
            if street:
                 address = district + ", " + street + " " + house_number + ", " + zipcode + " " + region + ", Germany"
            else:
                address = district + ", " + house_number + ", " + zipcode + " " + region + ", Germany"
        else:
            if street:
                 address = street + " " + house_number + ", " + zipcode + " " + region + ", Germany"
            else:
                address = house_number + ", " + zipcode + " " + region + ", Germany"

        rent = response.meta.get('rent')
        rent = rent.split()[0]
        rent = rent.replace(".", "")
        rent = rent.replace(",", ".")
        rent = round(float(rent))
        rent = int(rent)

        floor = response.meta.get('floor')
        floor = floor.replace(" ", "")

        amenities = response.meta.get('amenities')
        balcony = None
        elevator = None
        for item in amenities:
            if "balkon" in item.lower():
                balcony = True
            if "aufzug" in item.lower():
                elevator = True

        square_meters = response.meta.get('square_meters')
        square_meters = square_meters.split()[0]
        square_meters = square_meters.replace(".", "")
        square_meters = square_meters.replace(",", ".")
        square_meters = round(float(square_meters))
        square_meters = int(square_meters)

        room_count = response.meta.get('room_count')
        room_count = room_count.replace(",", ".")
        room_count = round(float(room_count))
        room_count = int(room_count)

        images_all = response.css('div.object_images div.fotorama img::attr(src)').extract()
        images = []
        for image in images_all:
            if image.startswith("/"):
                image = "https://www.wbg-kontakt.de" + image
            else:
                image = "https://www.wbg-kontakt.de/" + image
            if "default.jpg" not in image:
                images.append(image)

        external_id = response.css('div.details_list.details_list_object div.objectid div.fieldValue::text')[0].extract()
        external_id = external_id.replace('\n', '')
        external_id = external_id.strip()

        energy_label = response.css('div.immo_contact_details_item div.detail_energy_efficiency_classc div.fieldValue::text').extract()

        prices_keys = response.css('div.details_list.details_list_price div.detail_incidentals div.fieldLabel::text').extract()
        prices_values = response.css('div.details_list.details_list_price div.detail_incidentals div.fieldValue::text').extract()
        prices = dict(zip(prices_keys, prices_values))
        utilities = prices['Neben\xadkosten:']
        utilities = utilities.replace('\n', '')
        utilities = utilities.split()[0]
        utilities = utilities.replace(".", "")
        utilities = utilities.replace(",", ".")
        utilities = round(float(utilities))
        utilities = int(utilities)
        if utilities == 0:
            utilities = None

        heating_cost = prices['Heiz\xadkosten:']
        heating_cost = heating_cost.replace('\n', '')
        heating_cost = heating_cost.split()[0]
        heating_cost = heating_cost.replace(".", "")
        heating_cost = heating_cost.replace(",", ".")
        heating_cost = round(float(heating_cost))
        heating_cost = int(heating_cost)
        if heating_cost == 0:
            heating_cost = None

        description = response.css('p.text-features::text').extract()

        lat_lng_script = response.xpath('.//script/text()').extract()
        lat_lng = None
        for item in lat_lng_script:
            if "myLatLng" in item:
                lat_lng = item
        if lat_lng:
            lat_lng = lat_lng.split("myLatLng")[1]
            lat_lng = lat_lng.split("};")[0]
            lat_lng = lat_lng.split(',lng:')
            latitude = lat_lng[0]
            latitude = latitude.split('lat:')[1]
            longitude = lat_lng[1]
        else:
            latitude = None
            longitude = None
        if not latitude:
            longitude, latitude = extract_location_from_address(address)
            longitude = str(longitude)
            latitude = str(latitude)

        property_type = 'apartment'

        landlord_name = response.css('div.contactPerson-data::text')[0].extract()
        landlord_name = " ".join(landlord_name.split())
        landlord_number = response.css('div.contactPerson-tel div.fieldValue a::text')[0].extract()
        landlord_email = response.css('div.contactPerson-email div.fieldValue a::text')[0].extract()

        # Enforces rent between 0 and 40,000 please dont delete these lines
        if int(rent) <= 0 and int(rent) > 40000:
            return

        # # MetaData
        item_loader.add_value("external_link", response.url) # String
        item_loader.add_value("external_source", self.external_source) # String

        item_loader.add_value("external_id", external_id) # String
        item_loader.add_value("position", self.position) # Int
        item_loader.add_value("title", title) # String
        item_loader.add_value("description", description) # String

        # # Property Details
        item_loader.add_value("city", city) # String
        item_loader.add_value("zipcode", zipcode) # String
        item_loader.add_value("address", address) # String
        item_loader.add_value("latitude", latitude) # String
        item_loader.add_value("longitude", longitude) # String
        item_loader.add_value("floor", floor) # String
        item_loader.add_value("property_type", property_type) # String => ["apartment", "house", "room", "student_apartment", "studio"]
        item_loader.add_value("square_meters", square_meters) # Int
        item_loader.add_value("room_count", room_count) # Int
        #item_loader.add_value("bathroom_count", bathroom_count) # Int

        # item_loader.add_value("available_date", available_date) # String => date_format

        #item_loader.add_value("pets_allowed", pets_allowed) # Boolean
        #item_loader.add_value("furnished", furnished) # Boolean
        #item_loader.add_value("parking", parking) # Boolean
        item_loader.add_value("elevator", elevator) # Boolean
        item_loader.add_value("balcony", balcony) # Boolean
        #item_loader.add_value("terrace", terrace) # Boolean
        #item_loader.add_value("swimming_pool", swimming_pool) # Boolean
        #item_loader.add_value("washing_machine", washing_machine) # Boolean
        #item_loader.add_value("dishwasher", dishwasher) # Boolean

        # # Images
        item_loader.add_value("images", images) # Array
        item_loader.add_value("external_images_count", len(images)) # Int
        #item_loader.add_value("floor_plan_images", floor_plan_images) # Array

        # # Monetary Status
        item_loader.add_value("rent", rent) # Int
        #item_loader.add_value("deposit", deposit) # Int
        #item_loader.add_value("prepaid_rent", prepaid_rent) # Int
        item_loader.add_value("utilities", utilities) # Int
        item_loader.add_value("currency", "EUR") # String

        #item_loader.add_value("water_cost", water_cost) # Int
        item_loader.add_value("heating_cost", heating_cost) # Int

        item_loader.add_value("energy_label", energy_label) # String

        # # LandLord Details
        item_loader.add_value("landlord_name", landlord_name) # String
        item_loader.add_value("landlord_phone", landlord_number) # String
        item_loader.add_value("landlord_email", landlord_email) # String

        self.position += 1
        yield item_loader.load_item()
